#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import base64
import json
import math
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import rosbag
import rospy
from cyber_record.record import Record
from geometry_msgs.msg import TransformStamped
from google.protobuf import json_format
from nav_msgs.msg import Odometry
from sensor_msgs.msg import Imu, NavSatFix, NavSatStatus
from std_msgs.msg import Header, String, UInt8MultiArray
from tf2_msgs.msg import TFMessage

from record2bag_mid360 import LivoxConverter, try_import_driver


def normalize_timestamp_ns(ts_ns):
    return max(0, int(ts_ns))


def pointcloud_message_to_frame(message, t_ns):
    pts = []
    for p in message.point:
        time_ns = int(getattr(p, "timestamp", 0))
        if time_ns <= 0 and hasattr(p, "timestamp_sec"):
            time_ns = int(float(p.timestamp_sec) * 1e9)
        pts.append([
            float(getattr(p, "x", 0.0)),
            float(getattr(p, "y", 0.0)),
            float(getattr(p, "z", 0.0)),
            float(getattr(p, "intensity", 0.0)),
            time_ns,
            int(getattr(p, "ring", 0)),
        ])
    return {
        "timestamp": normalize_timestamp_ns(t_ns),
        "points": pts,
        "original_timestamp": t_ns,
    }


def imu_message_to_dict(message, t_ns):
    return {
        "timestamp": normalize_timestamp_ns(t_ns),
        "linear_acc": [
            float(message.linear_acceleration.x),
            float(message.linear_acceleration.y),
            float(message.linear_acceleration.z),
        ],
        "angular_vel": [
            float(message.angular_velocity.x),
            float(message.angular_velocity.y),
            float(message.angular_velocity.z),
        ],
        "original_timestamp": t_ns,
    }


def ns_to_ros_time(t_ns):
    return rospy.Time.from_sec(max(0, int(t_ns)) * 1e-9)


def cyber_header_to_ros(header, cyber_header, fallback_ns):
    if cyber_header is not None and cyber_header.ByteSize() > 0:
        if cyber_header.timestamp_sec > 0:
            header.stamp = rospy.Time.from_sec(cyber_header.timestamp_sec)
        else:
            header.stamp = ns_to_ros_time(fallback_ns)
        if cyber_header.frame_id:
            header.frame_id = cyber_header.frame_id
        if hasattr(cyber_header, "sequence_num"):
            header.seq = int(cyber_header.sequence_num)
    else:
        header.stamp = ns_to_ros_time(fallback_ns)


def to_imu(msg, t_ns, frame_id=None):
    ros_imu = Imu()
    header = Header()
    cyber_header = msg.header if msg.HasField("header") else None
    resolved_frame = frame_id or "imu"
    if frame_id is None:
        if cyber_header is not None and cyber_header.frame_id:
            resolved_frame = cyber_header.frame_id
        elif cyber_header is not None and cyber_header.module_name:
            resolved_frame = cyber_header.module_name
    cyber_header_to_ros(header, cyber_header, t_ns)
    header.frame_id = resolved_frame
    ros_imu.header = header

    ros_imu.linear_acceleration.x = _safe_float(msg.linear_acceleration.x)
    ros_imu.linear_acceleration.y = _safe_float(msg.linear_acceleration.y)
    ros_imu.linear_acceleration.z = _safe_float(msg.linear_acceleration.z)
    ros_imu.angular_velocity.x = _safe_float(msg.angular_velocity.x)
    ros_imu.angular_velocity.y = _safe_float(msg.angular_velocity.y)
    ros_imu.angular_velocity.z = _safe_float(msg.angular_velocity.z)
    return ros_imu


def to_livox_imu(imu_dict, imu_frame):
    ros_imu = Imu()
    header = Header()
    header.stamp = rospy.Time.from_sec(imu_dict["timestamp"] * 1e-9)
    header.frame_id = imu_frame
    ros_imu.header = header
    ros_imu.linear_acceleration.x = imu_dict["linear_acc"][0]
    ros_imu.linear_acceleration.y = imu_dict["linear_acc"][1]
    ros_imu.linear_acceleration.z = imu_dict["linear_acc"][2]
    ros_imu.angular_velocity.x = imu_dict["angular_vel"][0]
    ros_imu.angular_velocity.y = imu_dict["angular_vel"][1]
    ros_imu.angular_velocity.z = imu_dict["angular_vel"][2]
    return ros_imu


def _safe_float(v, fallback=0.0):
    """Return fallback if v is NaN or cannot be converted."""
    try:
        f = float(v)
        return fallback if math.isnan(f) else f
    except Exception:
        return fallback


def _angular_velocity_body(pose):
    """
    Return (wx, wy, wz) in the vehicle body frame.

    Apollo LocalizationEstimate stores two angular-velocity fields:
      - angular_velocity     : ENU world frame  (often left unset → nan)
      - angular_velocity_vrf : vehicle Right/Forward/Up frame (typically set)

    ROS Odometry.twist.angular is expected in child_frame_id (base_link), so
    the vehicle-frame value is the correct one to use.  The VRF axes
    (Right, Forward, Up) map to ROS FLU (Forward, Left, Up) as:
        ROS x (forward) =  VRF y (forward)
        ROS y (left)    = -VRF x (right)
        ROS z (up)      =  VRF z (up)
    """
    if pose.HasField("angular_velocity_vrf"):
        vrf = pose.angular_velocity_vrf
        wx = _safe_float(vrf.x)
        wy = _safe_float(vrf.y)
        wz = _safe_float(vrf.z)
        # VRF → FLU rotation
        return wy, -wx, wz

    # Fall back to ENU angular_velocity only when vrf is absent
    if pose.HasField("angular_velocity"):
        av = pose.angular_velocity
        wx = _safe_float(av.x)
        wy = _safe_float(av.y)
        wz = _safe_float(av.z)
        if any(v != 0.0 for v in (wx, wy, wz)):
            return wx, wy, wz

    return 0.0, 0.0, 0.0


# Fallback position variance (m²) used when uncertainty.position_std_dev is absent/nan
_POS_VAR_DEFAULT = 0.1 ** 2   # 0.1 m
# Fallback orientation variance (rad²) used when orientation_std_dev is absent/nan
_ORI_VAR_DEFAULT = 0.1 ** 2   # ~5.7°


def _fill_pose_covariance(odom, uncertainty):
    """
    Map Apollo Uncertainty std-devs → Odometry 6×6 pose covariance (row-major).
    Index layout: [x, y, z, roll, pitch, yaw].
    Only diagonal entries are filled; off-diagonal terms are left as 0.
    """
    cov = [0.0] * 36

    if uncertainty is not None and uncertainty.HasField("position_std_dev"):
        ps = uncertainty.position_std_dev
        cov[0]  = _safe_float(ps.x, _POS_VAR_DEFAULT) ** 2
        cov[7]  = _safe_float(ps.y, _POS_VAR_DEFAULT) ** 2
        cov[14] = _safe_float(ps.z, _POS_VAR_DEFAULT) ** 2
    else:
        cov[0] = cov[7] = cov[14] = _POS_VAR_DEFAULT

    if uncertainty is not None and uncertainty.HasField("orientation_std_dev"):
        os_ = uncertainty.orientation_std_dev
        cov[21] = _safe_float(os_.x, _ORI_VAR_DEFAULT) ** 2
        cov[28] = _safe_float(os_.y, _ORI_VAR_DEFAULT) ** 2
        cov[35] = _safe_float(os_.z, _ORI_VAR_DEFAULT) ** 2
    else:
        cov[21] = cov[28] = cov[35] = _ORI_VAR_DEFAULT

    odom.pose.covariance = cov


def to_odometry(msg, t_ns):
    odom = Odometry()
    cyber_header_to_ros(odom.header, msg.header if msg.HasField("header") else None, t_ns)
    odom.header.frame_id = odom.header.frame_id or "map"
    odom.child_frame_id = "base_link"

    pose = msg.pose
    odom.pose.pose.position.x = float(pose.position.x)
    odom.pose.pose.position.y = float(pose.position.y)
    odom.pose.pose.position.z = float(pose.position.z)
    odom.pose.pose.orientation.x = float(pose.orientation.qx)
    odom.pose.pose.orientation.y = float(pose.orientation.qy)
    odom.pose.pose.orientation.z = float(pose.orientation.qz)
    odom.pose.pose.orientation.w = float(pose.orientation.qw)

    odom.twist.twist.linear.x = _safe_float(pose.linear_velocity.x)
    odom.twist.twist.linear.y = _safe_float(pose.linear_velocity.y)
    odom.twist.twist.linear.z = _safe_float(pose.linear_velocity.z)

    wx, wy, wz = _angular_velocity_body(pose)
    odom.twist.twist.angular.x = wx
    odom.twist.twist.angular.y = wy
    odom.twist.twist.angular.z = wz

    uncertainty = msg.uncertainty if msg.HasField("uncertainty") else None
    _fill_pose_covariance(odom, uncertainty)

    return odom


def decode_raw_bytes(data):
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception:
            return data.encode("utf-8", errors="ignore")
    return bytes(data)


def _parse_gga(sentence):
    """
    Parse a $GPGGA / $GNGGA NMEA sentence.
    Returns a dict with keys: lat, lon, alt, status, num_sv, hdop,
    or None if the sentence is invalid / not a GGA.

    GGA quality field → NavSatStatus:
        0 = invalid       → STATUS_NO_FIX (-1)
        1 = SPS fix       → STATUS_FIX    (0)
        2 = DGPS          → STATUS_SBAS_FIX (1)
        4 = RTK fixed     → STATUS_GBAS_FIX (2)
        5 = RTK float     → STATUS_GBAS_FIX (2)  # closest ROS equivalent
    """
    try:
        # strip checksum
        body = sentence.strip()
        if "*" in body:
            body = body[:body.index("*")]
        fields = body.split(",")
        if len(fields) < 10:
            return None
        tag = fields[0].upper()
        if not tag.endswith("GGA"):
            return None

        quality = int(fields[6]) if fields[6] else 0
        if quality == 0:
            return None

        # latitude: ddmm.mmmm N/S
        lat_raw = fields[2]
        lat_hemi = fields[3]
        if not lat_raw:
            return None
        lat_deg = float(lat_raw[:2])
        lat_min = float(lat_raw[2:])
        lat = lat_deg + lat_min / 60.0
        if lat_hemi == "S":
            lat = -lat

        # longitude: dddmm.mmmm E/W
        lon_raw = fields[4]
        lon_hemi = fields[5]
        if not lon_raw:
            return None
        lon_deg = float(lon_raw[:3])
        lon_min = float(lon_raw[3:])
        lon = lon_deg + lon_min / 60.0
        if lon_hemi == "W":
            lon = -lon

        alt = float(fields[9]) if fields[9] else 0.0
        num_sv = int(fields[7]) if fields[7] else 0
        hdop = float(fields[8]) if fields[8] else 99.0

        _quality_to_status = {
            1: NavSatStatus.STATUS_FIX,
            2: NavSatStatus.STATUS_SBAS_FIX,
            4: NavSatStatus.STATUS_GBAS_FIX,
            5: NavSatStatus.STATUS_GBAS_FIX,
        }
        status = _quality_to_status.get(quality, NavSatStatus.STATUS_FIX)

        return {"lat": lat, "lon": lon, "alt": alt,
                "status": status, "num_sv": num_sv, "hdop": hdop}
    except Exception:
        return None


# Horizontal position variance (m²) per NavSatStatus used for NavSatFix.
# These are conservative defaults; a proper RTK engine would supply better values.
_NAVSATFIX_COV = {
    NavSatStatus.STATUS_GBAS_FIX:  0.02 ** 2,   # RTK fixed/float  ~2 cm
    NavSatStatus.STATUS_SBAS_FIX:  0.5  ** 2,   # DGPS             ~0.5 m
    NavSatStatus.STATUS_FIX:       2.5  ** 2,   # SPS              ~2.5 m
}


def gga_to_navsatfix(sentence, t_ns, frame_id="gps"):
    """Convert a single GGA NMEA sentence to sensor_msgs/NavSatFix, or None."""
    gga = _parse_gga(sentence)
    if gga is None:
        return None

    fix = NavSatFix()
    fix.header.stamp = ns_to_ros_time(t_ns)
    fix.header.frame_id = frame_id

    fix.status.status = gga["status"]
    fix.status.service = NavSatStatus.SERVICE_GPS

    fix.latitude  = gga["lat"]
    fix.longitude = gga["lon"]
    fix.altitude  = gga["alt"]

    var = _NAVSATFIX_COV.get(gga["status"], 2.5 ** 2)
    fix.position_covariance = [var, 0, 0,
                               0, var, 0,
                               0, 0, var * 4]   # vertical ~2× worse
    fix.position_covariance_type = NavSatFix.COVARIANCE_TYPE_APPROXIMATED

    return fix


def to_raw_data_messages(msg, topic, t_ns):
    payload = decode_raw_bytes(msg.data)
    outputs = []

    if topic.endswith("/raw_data") or payload.startswith(b"$"):
        text = payload.decode("utf-8", errors="replace")
        ros_str = String()
        ros_str.data = text
        outputs.append((topic, ros_str))

        # Also emit a NavSatFix on <topic_base>/fix if this is a GGA sentence
        fix = gga_to_navsatfix(text.strip(), t_ns)
        if fix is not None:
            fix_topic = topic.replace("/raw_data", "/fix")
            outputs.append((fix_topic, fix))

        return outputs

    ros_bytes = UInt8MultiArray()
    ros_bytes.data = list(payload)
    outputs.append((topic, ros_bytes))
    return outputs


def to_json_string(msg):
    ros_str = String()
    ros_str.data = json.dumps(
        json_format.MessageToDict(msg, preserving_proto_field_name=True),
        ensure_ascii=False,
    )
    return ros_str


def to_proto_bytes(msg):
    ros_bytes = UInt8MultiArray()
    ros_bytes.data = list(msg.SerializeToString())
    return ros_bytes


def to_tf_message(msg, t_ns):
    tf_msg = TFMessage()
    for item in msg.transforms:
        stamped = TransformStamped()
        cyber_header_to_ros(
            stamped.header,
            item.header if item.HasField("header") else None,
            t_ns,
        )
        stamped.child_frame_id = item.child_frame_id
        stamped.transform.translation.x = float(item.transform.translation.x)
        stamped.transform.translation.y = float(item.transform.translation.y)
        stamped.transform.translation.z = float(item.transform.translation.z)
        stamped.transform.rotation.x = float(item.transform.rotation.qx)
        stamped.transform.rotation.y = float(item.transform.rotation.qy)
        stamped.transform.rotation.z = float(item.transform.rotation.qz)
        stamped.transform.rotation.w = float(item.transform.qw)
        tf_msg.transforms.append(stamped)
    return tf_msg


class RecordConverter(object):
    def __init__(self, args, livox_converter=None):
        self.args = args
        self.livox = livox_converter

    def convert(self, msg, topic, t_ns, use_proto_fallback=True):
        if topic == self.args.pc_topic and self.livox is not None and msg.__class__.__name__ == "PointCloud":
            frame = pointcloud_message_to_frame(msg, t_ns)
            custom_msg = self.livox.to_custom_msg(frame)
            if custom_msg is None:
                return []
            record_stamp = ns_to_ros_time(t_ns)
            custom_msg.header.stamp = record_stamp
            return [(self.livox.lidar_topic, custom_msg)]

        if topic == self.args.imu_topic and msg.__class__.__name__ == "Imu" and self.livox is not None:
            imu_dict = imu_message_to_dict(msg, t_ns)
            ros_imu = to_livox_imu(imu_dict, self.livox.imu_frame)
            return [(self.livox.imu_topic_out, ros_imu)]

        msg_type = msg.__class__.__name__
        if msg_type == "Imu":
            return [(topic, to_imu(msg, t_ns))]
        if msg_type == "LocalizationEstimate":
            return [(topic, to_odometry(msg, t_ns))]
        if msg_type == "RawData":
            return to_raw_data_messages(msg, topic, t_ns)
        if msg_type == "EpochObservation":
            return [(topic, to_json_string(msg))]
        if msg_type == "TransformStampeds":
            return [(topic, to_tf_message(msg, t_ns))]
        if use_proto_fallback:
            return [(topic, to_proto_bytes(msg))]
        return []


def convert_record(input_record, output_bag, converter):
    stats = Counter()
    skipped = Counter()

    with Record(input_record) as record, rosbag.Bag(output_bag, "w") as bag:
        for topic, msg, t in record.read_messages():
            try:
                outputs = converter.convert(
                    msg,
                    topic,
                    t,
                    use_proto_fallback=not converter.args.no_proto_fallback,
                )
            except Exception as exc:
                skipped[f"{topic} ({msg.__class__.__name__}): {exc}"] += 1
                continue

            for out_topic, ros_msg in outputs:
                stamp = ns_to_ros_time(t)
                if hasattr(ros_msg, "header"):
                    ros_msg.header.stamp = stamp
                bag.write(out_topic, ros_msg, stamp)
                stats[f"{out_topic} -> {ros_msg._type}"] += 1

    return stats, skipped


def collect_record_files(input_path):
    if os.path.isfile(input_path):
        return [input_path]

    files = []
    for name in sorted(os.listdir(input_path)):
        path = os.path.join(input_path, name)
        if os.path.isfile(path):
            files.append(path)
    return files


def make_output_path(output_path, batch_mode, index):
    if not batch_mode:
        return output_path
    return os.path.join(output_path, f"{index}.bag")


def build_converter(args):
    use_driver2, CustomMsg, CustomPoint = try_import_driver(args.use_driver2)
    livox_converter = LivoxConverter(
        use_driver2=use_driver2,
        CustomMsg=CustomMsg,
        CustomPoint=CustomPoint,
        lidar_frame=args.lidar_frame,
        imu_frame=args.imu_frame,
        lidar_topic=args.lidar_topic_out,
        imu_topic_out=args.imu_topic_out,
        auto_unit_detect=not args.no_auto_unit_detect,
        point_unit=args.point_unit,
        drop_zero_points=not args.keep_zero_points,
    )
    return RecordConverter(args, livox_converter=livox_converter)


def print_conversion_result(input_record, output_bag, stats, skipped):
    print(f"[INFO] Converting {input_record} -> {output_bag}")
    for key, count in sorted(stats.items()):
        print(f"  {count}\t{key}")
    if skipped:
        print("[WARN] Skipped messages:")
        for key, count in sorted(skipped.items()):
            print(f"  {count}\t{key}")
    print(f"[OK] Wrote {output_bag}")


def convert_one(args, input_record, output_bag):
    converter = build_converter(args)
    stats, skipped = convert_record(input_record, output_bag, converter)
    print_conversion_result(input_record, output_bag, stats, skipped)
    return output_bag


def convert_one_worker(args, input_record, output_bag):
    return convert_one(args, input_record, output_bag)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert Apollo Cyber record topics to ROS bag."
    )
    parser.add_argument("input_record", help="Input .record file or directory")
    parser.add_argument("output_bag", help="Output .bag file or output directory in batch mode")
    parser.add_argument(
        "--no-proto-fallback",
        action="store_true",
        help="Skip unknown Apollo message types instead of writing ByteMultiArray",
    )
    parser.add_argument("--use_driver2", action="store_true", help="Use livox_ros_driver2 message type")
    parser.add_argument("--imu_topic", default="/apollo/sensor/mid_360/Imu", help="Mid360 IMU topic in record")
    parser.add_argument("--pc_topic", default="/apollo/sensor/mid_360/PointCloud2", help="Mid360 point cloud topic in record")
    parser.add_argument("--lidar_topic_out", default="/livox/lidar", help="Output lidar topic")
    parser.add_argument("--imu_topic_out", default="/livox/imu", help="Output Mid360 imu topic")
    parser.add_argument("--lidar_frame", default="livox_frame", help="Lidar frame_id")
    parser.add_argument("--imu_frame", default="livox_imu_frame", help="Mid360 imu frame_id")
    parser.add_argument("--no_auto_unit_detect", action="store_true", help="Disable auto unit detection for Mid360 points")
    parser.add_argument("--point_unit", choices=["auto", "m", "mm"], default="m", help="Input point distance unit")
    parser.add_argument("--keep_zero_points", action="store_true", help="Keep zero (0,0,0) Mid360 points")
    parser.add_argument("--jobs", type=int, default=4, help="Parallel conversions in batch mode")
    return parser.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input_record):
        print(f"[FATAL] Input path not found: {args.input_record}", file=sys.stderr)
        return 1

    batch_mode = os.path.isdir(args.input_record)
    if batch_mode and os.path.isfile(args.output_bag):
        print(f"[FATAL] Output path must be a directory in batch mode: {args.output_bag}", file=sys.stderr)
        return 1

    input_records = collect_record_files(args.input_record)
    if not input_records:
        print(f"[FATAL] No record files found: {args.input_record}", file=sys.stderr)
        return 1

    if batch_mode:
        os.makedirs(args.output_bag, exist_ok=True)

    jobs = max(1, int(args.jobs))
    if not batch_mode and jobs != 1:
        print("[WARN] --jobs is only used in batch mode. Single-file conversion will run with one process.")
        jobs = 1
    if batch_mode:
        jobs = min(jobs, len(input_records))

    try:
        try_import_driver(args.use_driver2)
    except Exception as exc:
        print(f"[FATAL] Cannot import Livox driver (use_driver2={args.use_driver2}). {exc}", file=sys.stderr)
        return 1

    failed = []
    total = len(input_records)
    if batch_mode:
        print(f"[INFO] Batch mode: found {total} files in {args.input_record}")
        print(f"[INFO] Parallel jobs: {jobs}")
        print(f"[INFO] Mid360: {args.pc_topic} -> {args.lidar_topic_out}, {args.imu_topic} -> {args.imu_topic_out}")

    if jobs == 1:
        rospy.init_node("record2bag_all", anonymous=True, disable_signals=True)
        for index, input_record in enumerate(input_records):
            output_bag = make_output_path(args.output_bag, batch_mode, index)
            if batch_mode:
                print(f"[INFO] [{index + 1}/{total}]")
            try:
                convert_one(args, input_record, output_bag)
            except Exception as exc:
                failed.append((input_record, str(exc)))
                print(f"[ERROR] Failed to convert {input_record}: {exc}")
    else:
        tasks = {}
        with ProcessPoolExecutor(max_workers=jobs) as executor:
            for index, input_record in enumerate(input_records):
                output_bag = make_output_path(args.output_bag, batch_mode, index)
                print(f"[INFO] Submit [{index + 1}/{total}] {input_record} -> {output_bag}")
                future = executor.submit(convert_one_worker, args, input_record, output_bag)
                tasks[future] = input_record

            for future in as_completed(tasks):
                input_record = tasks[future]
                try:
                    output_bag = future.result()
                    print(f"[OK] Finished: {output_bag}")
                except Exception as exc:
                    failed.append((input_record, str(exc)))
                    print(f"[ERROR] Failed to convert {input_record}: {exc}")

    if failed:
        print(f"[FATAL] {len(failed)}/{total} conversions failed:", file=sys.stderr)
        for input_record, error in failed:
            print(f"  - {input_record}: {error}", file=sys.stderr)
        return 1

    print("[OK] All conversions completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
