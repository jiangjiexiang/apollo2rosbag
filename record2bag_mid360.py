#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import numpy as np
import rosbag
import rospy
from sensor_msgs.msg import Imu
from std_msgs.msg import Header
from cyber_record.record import Record

UINT32_MAX = (1 << 32) - 1

def try_import_driver(use_driver2):
    if use_driver2:
        from livox_ros_driver2.msg import CustomMsg, CustomPoint
        return True, CustomMsg, CustomPoint
    else:
        from livox_ros_driver.msg import CustomMsg, CustomPoint
        return False, CustomMsg, CustomPoint

# -------- utils --------
def normalize_timestamp_ns(ts_ns):
    # Apollo 的 t 也可能为负；这里统一裁剪
    return max(0, int(ts_ns))

def safe_uint32(x):
    return np.uint32(max(0, min(int(x), UINT32_MAX)))

def clip_u8(x):
    return np.uint8(max(0, min(int(x), 255)))

def clip_u16(x):
    return np.uint16(max(0, min(int(x), 65535)))

def detect_scale(points_xyz, warn_prefix=""):
    """
    粗略检测单位（m vs mm）。若坐标范围非常大（> 10000），判做 mm -> m
    points_xyz: (N,3) ndarray
    """
    if points_xyz.size == 0:
        return 1.0
    max_abs = np.max(np.abs(points_xyz))
    if max_abs > 1e5:    # 非常大，基本可以断定是 mm
        scale = 1.0 / 1000.0
        print(f"{warn_prefix}[INFO] Auto-detected point unit seems mm. Apply scale 1/1000.")
    else:
        scale = 1.0
    return scale

# -------- core --------
def extract_data(record_file, imu_topic, pc_topic):
    """
    读取 Apollo cyber_record，提取点云 & IMU
    返回：
      pointcloud_frames: list(dict)
      imu_data: list(dict)
    """
    pointcloud_frames = []
    imu_data = []

    with Record(record_file) as record:
        for topic, message, t in record.read_messages():
            t_ns = normalize_timestamp_ns(t)

            if topic == pc_topic:
                if hasattr(message, 'point'):
                    pts = []
                    for p in message.point:
                        pts.append([
                            float(getattr(p, 'x', 0.0)),
                            float(getattr(p, 'y', 0.0)),
                            float(getattr(p, 'z', 0.0)),
                            float(getattr(p, 'intensity', 0.0)),
                            int(getattr(p, 'timestamp', 0)),   # ns
                            int(getattr(p, 'ring', 0))
                        ])
                    pointcloud_frames.append({
                        'timestamp': t_ns,    # ns
                        'points': pts,
                        'original_timestamp': t
                    })

            elif topic == imu_topic:
                if hasattr(message, 'linear_acceleration') and hasattr(message, 'angular_velocity'):
                    imu_data.append({
                        'timestamp': t_ns,
                        'linear_acc': [
                            float(message.linear_acceleration.x),
                            float(message.linear_acceleration.y),
                            float(message.linear_acceleration.z)
                        ],
                        'angular_vel': [
                            float(message.angular_velocity.x),
                            float(message.angular_velocity.y),
                            float(message.angular_velocity.z)
                        ],
                        'original_timestamp': t
                    })

    return pointcloud_frames, imu_data


class LivoxConverter(object):
    def __init__(self,
                 use_driver2,
                 CustomMsg, CustomPoint,
                 lidar_frame="livox_frame",
                 imu_frame="livox_imu_frame",
                 lidar_topic="/livox/lidar",
                 imu_topic_out="/livox/imu",
                 auto_unit_detect=True,
                 drop_zero_points=True,
                 debug=False):
        self.use_driver2 = use_driver2
        self.CustomMsg = CustomMsg
        self.CustomPoint = CustomPoint
        self.lidar_frame = lidar_frame
        self.imu_frame = imu_frame
        self.lidar_topic = lidar_topic
        self.imu_topic_out = imu_topic_out
        self.auto_unit_detect = auto_unit_detect
        self.drop_zero_points = drop_zero_points
        self.debug = debug

        # 相对基准（ns）：第一个有效帧第一点的时间戳
        self.global_base_ns = None
        # 自动单位缩放
        self.scale = 1.0
        # 仅打印一次统计
        self.first_frame_logged = False

    def _init_baseline_and_scale_if_needed(self, pc):
        """在第一次调用时初始化 global_base_ns 和 scale"""
        if self.global_base_ns is not None:
            return

        # 取首帧的第一个有时间戳的点
        time_ns_list = [pt[4] for pt in pc['points'] if pt[4] > 0]
        if len(time_ns_list) == 0:
            self.global_base_ns = pc['timestamp']
        else:
            self.global_base_ns = min(time_ns_list)

        if self.global_base_ns <= 0:
            self.global_base_ns = 0

        if self.auto_unit_detect:
            pts_np = np.array([[p[0], p[1], p[2]] for p in pc['points']], dtype=np.float64)
            self.scale = detect_scale(pts_np, warn_prefix="[UNIT] ")

    def to_custom_msg(self, pc, lidar_id=1):
        """
        输入一帧 Apollo 点云字典，输出 Livox CustomMsg
        """
        if len(pc['points']) == 0:
            return None

        self._init_baseline_and_scale_if_needed(pc)

        # 1) 过滤/整理点并排序（按时间）
        pts = [p for p in pc['points']]
        # 如果很多点 time_ns = 0，用帧时间代替，避免负 offset
        frame_ts_ns = normalize_timestamp_ns(pc['timestamp'])
        pts_clean = []
        for p in pts:
            x, y, z, intensity, time_ns, ring = p
            if time_ns <= 0:
                time_ns = frame_ts_ns
            pts_clean.append([x, y, z, intensity, time_ns, ring])

        pts_clean.sort(key=lambda x: x[4])  # 按时间戳排序（ns）

        first_time_ns = pts_clean[0][4]
        if first_time_ns <= 0:
            first_time_ns = frame_ts_ns

        # 2) Header 使用真实时间（方便与其他传感器同步）
        header = Header()
        header.stamp = rospy.Time.from_sec(first_time_ns * 1e-9)
        header.frame_id = self.lidar_frame

        # 3) 计算 CustomMsg 的 timebase
        msg = self.CustomMsg()
        msg.header = header

        if self.use_driver2:
            # v2: uint64 ns, 可以直接用真实时间
            msg.timebase = np.uint64(first_time_ns)
            msg.lidar_id = clip_u8(lidar_id)
        else:
            # v1: uint32 us，需要相对 global_base_ns 才不溢出
            timebase_us = (first_time_ns - self.global_base_ns) // 1000
            msg.timebase = safe_uint32(timebase_us)
            msg.lidar_id = clip_u8(lidar_id)

        # 4) 填充 points
        points_out = []
        valid_cnt = 0
        for p in pts_clean:
            x, y, z, intensity, time_ns, ring = p

            if self.drop_zero_points and abs(x) < 1e-7 and abs(y) < 1e-7 and abs(z) < 1e-7:
                continue

            # 单位缩放
            x *= 0.001
            y *= 0.001
            z *= 0.001

            # offset_time 以本帧第一点为基准（us）
            offset_us = max(0, (time_ns - first_time_ns) // 1000)

            cp = self.CustomPoint()
            if self.use_driver2:
                # livox_ros_driver2/CustomPoint:
                # uint32 offset_time
                # float32 x y z
                # uint8 reflectivity, tag, line
                cp.offset_time = safe_uint32(offset_us)
                cp.x = float(x)
                cp.y = float(y)
                cp.z = float(z)
                cp.reflectivity = clip_u8(np.clip(intensity, 0, 255))
                cp.tag = clip_u8(0)
                cp.line = clip_u8(ring)
            else:
                # livox_ros_driver/CustomPoint:
                # float32 x y z
                # uint8 reflectivity
                # uint8 tag
                # uint16 line
                # uint32 offset_time
                cp.x = float(x)
                cp.y = float(y)
                cp.z = float(z)
                cp.reflectivity = clip_u8(np.clip(intensity, 0, 255))
                cp.tag = clip_u8(0)
                cp.line = clip_u16(ring)
                cp.offset_time = safe_uint32(offset_us)

            points_out.append(cp)
            valid_cnt += 1

        msg.points = points_out
        msg.point_num = safe_uint32(valid_cnt)

        # 5) debug 输出（仅首帧）
        if self.debug and (not self.first_frame_logged):
            self.first_frame_logged = True
            coords = np.array([[p.x, p.y, p.z] for p in points_out], dtype=np.float64)
            if coords.size > 0:
                print(f"[DEBUG] First frame stats:")
                print(f"  points: {len(points_out)} (valid after filter)")
                print(f"  timebase(ns): {first_time_ns}, msg.timebase: {msg.timebase}")
                print(f"  X range: {coords[:,0].min():.3f} ~ {coords[:,0].max():.3f}")
                print(f"  Y range: {coords[:,1].min():.3f} ~ {coords[:,1].max():.3f}")
                print(f"  Z range: {coords[:,2].min():.3f} ~ {coords[:,2].max():.3f}")
                if not self.use_driver2:
                    print(f"  (v1) global_base_ns: {self.global_base_ns}, "
                          f"timebase_us: {(first_time_ns - self.global_base_ns)//1000}")
                print(f"  scale used: {self.scale}")

        return msg

    def write_bag(self, bag_path, pointcloud_data, imu_data):
        with rosbag.Bag(bag_path, 'w') as bag:
            # 点云
            for pc in pointcloud_data:
                try:
                    msg = self.to_custom_msg(pc)
                    if msg is not None:
                        bag.write(self.lidar_topic, msg, t=msg.header.stamp)
                except Exception as e:
                    print(f"[ERROR] PointCloud write error: {e}")

            # IMU
            for imu in imu_data:
                try:
                    ros_imu = Imu()
                    header = Header()
                    header.stamp = rospy.Time.from_sec(imu['timestamp'] * 1e-9)
                    header.frame_id = self.imu_frame
                    ros_imu.header = header

                    ros_imu.linear_acceleration.x = imu['linear_acc'][0]
                    ros_imu.linear_acceleration.y = imu['linear_acc'][1]
                    ros_imu.linear_acceleration.z = imu['linear_acc'][2]
                    ros_imu.angular_velocity.x = imu['angular_vel'][0]
                    ros_imu.angular_velocity.y = imu['angular_vel'][1]
                    ros_imu.angular_velocity.z = imu['angular_vel'][2]
                    # orientation 未提供，保持默认 0
                    bag.write(self.imu_topic_out, ros_imu, header.stamp)
                except Exception as e:
                    print(f"[ERROR] IMU write error: {e}")


def parse_args():
    p = argparse.ArgumentParser(description="Convert Apollo Mid360 record to Livox CustomMsg rosbag.")
    p.add_argument("input_record", type=str, help="Input .record")
    p.add_argument("output_bag", type=str, help="Output .bag")
    p.add_argument("--use_driver2", action="store_true", help="Use livox_ros_driver2 message type")
    p.add_argument("--imu_topic", default="/apollo/sensor/mid_360/location/Imu", help="IMU topic in record")
    p.add_argument("--pc_topic", default="/apollo/sensor/mid_360/location/PointCloud2", help="PointCloud topic in record")
    p.add_argument("--lidar_topic_out", default="/livox/lidar", help="Output lidar topic")
    p.add_argument("--imu_topic_out", default="/livox/imu", help="Output imu topic")
    p.add_argument("--lidar_frame", default="livox_frame", help="lidar frame_id")
    p.add_argument("--imu_frame", default="livox_imu_frame", help="imu frame_id")
    p.add_argument("--no_auto_unit_detect", action="store_true", help="Disable auto unit detection")
    p.add_argument("--keep_zero_points", action="store_true", help="Keep zero (0,0,0) points")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input_record):
        print(f"[FATAL] Input file not found: {args.input_record}")
        sys.exit(1)

    # 离线写 bag 不一定需要 init_node，但 Fast-LIO 有时依赖 ros::Time::now/rosparam
    rospy.init_node('apollo_to_rosbag_mid360', anonymous=True, disable_signals=True)

    try:
        use_driver2, CustomMsg, CustomPoint = try_import_driver(args.use_driver2)
    except Exception as e:
        print(f"[FATAL] Cannot import Livox driver (use_driver2={args.use_driver2}). {e}")
        sys.exit(1)

    print(f"[INFO] Converting {args.input_record} -> {args.output_bag}")
    print(f"[INFO] Using {'livox_ros_driver2' if use_driver2 else 'livox_ros_driver'} message type")

    pointcloud_data, imu_data = extract_data(
        args.input_record,
        imu_topic=args.imu_topic,
        pc_topic=args.pc_topic
    )

    if len(pointcloud_data) == 0:
        print("[WARN] No pointcloud frames found!")
    if len(imu_data) == 0:
        print("[WARN] No imu data found!")

    converter = LivoxConverter(
        use_driver2=use_driver2,
        CustomMsg=CustomMsg,
        CustomPoint=CustomPoint,
        lidar_frame=args.lidar_frame,
        imu_frame=args.imu_frame,
        lidar_topic=args.lidar_topic_out,
        imu_topic_out=args.imu_topic_out,
        auto_unit_detect=(not args.no_auto_unit_detect),
        drop_zero_points=(not args.keep_zero_points),
        debug=args.debug
    )

    converter.write_bag(args.output_bag, pointcloud_data, imu_data)

    print("[OK] Conversion completed successfully.")


if __name__ == '__main__':
    main()
