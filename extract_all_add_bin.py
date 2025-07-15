import os
import csv
import struct
import cv2
import numpy as np
import shutil
from cyber_record.record import Record

# Topics, change it 
IMAGE_TOPICS = {
    "front": "/apollo/sensor/drivers/camera/surround_camera/front/compress",
    "rear": "/apollo/sensor/drivers/camera/surround_camera/rear/compress",
    "left": "/apollo/sensor/drivers/camera/surround_camera/left/compress"
}
IMU_TOPIC = "/vts/drivers/ins570d/checked_imu"
POINTCLOUD_TOPIC = "/vts/sensor/hesai40/compensated/PointCloud2"

def normalize_timestamp(t):
    return t / 1e9 if t > 1e18 else float(t)

def save_image(timestamp, data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    img_path = os.path.join(output_dir, f"{timestamp:.9f}.jpg")
    try:
        img_array = np.frombuffer(data, dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is not None:
            cv2.imwrite(img_path, img)
    except Exception as e:
        print(f"[Image] Error saving image at {timestamp}: {e}")

def save_pointcloud(timestamp, points, output_dir, pc_format="pcd"):
    os.makedirs(output_dir, exist_ok=True)

    if pc_format == "pcd":
        pcd_path = os.path.join(output_dir, f"{timestamp:.9f}.pcd")
        try:
            with open(pcd_path, 'w') as f:
                f.write("# .PCD v0.7 - Point Cloud Data file format\n")
                f.write("VERSION 0.7\n")
                f.write("FIELDS x y z intensity ring timestamp\n")
                f.write("SIZE 4 4 4 4 2 8\n")
                f.write("TYPE F F F F U F\n")
                f.write("COUNT 1 1 1 1 1 1\n")
                f.write(f"WIDTH {len(points)}\n")
                f.write("HEIGHT 1\n")
                f.write("VIEWPOINT 0 0 0 1 0 0 0\n")
                f.write(f"POINTS {len(points)}\n")
                f.write("DATA ascii\n")
                for pt in points:
                    x, y, z, intensity, ring, ts = pt
                    f.write(f"{x:.4f} {y:.4f} {z:.4f} {intensity:.1f} {int(ring)} {ts:.9f}\n")
        except Exception as e:
            print(f"[PointCloud] Error saving PCD at {timestamp}: {e}")

    elif pc_format == "bin":
        bin_path = os.path.join(output_dir, f"{timestamp:.9f}.bin")
        try:
            with open(bin_path, 'wb') as f:
                for pt in points:
                    x, y, z, intensity, ring, ts = pt
                    f.write(struct.pack('ffffHd', x, y, z, intensity, int(ring), ts))
        except Exception as e:
            print(f"[PointCloud] Error saving BIN at {timestamp}: {e}")

    else:
        raise ValueError(f"Unsupported point cloud format: {pc_format}")

def save_imu_csv(imu_data, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'acc_x', 'acc_y', 'acc_z', 'gyro_x', 'gyro_y', 'gyro_z'])
            for row in imu_data:
                writer.writerow(row)
    except Exception as e:
        print(f"[IMU] Error writing CSV: {e}")

def copy_synced_data(image_dir, pointcloud_dir, sync_image_dir, sync_pcd_dir, pc_format="pcd"):
    os.makedirs(sync_image_dir, exist_ok=True)
    os.makedirs(sync_pcd_dir, exist_ok=True)

    image_files = {f.split(".")[0]: f for f in os.listdir(image_dir) if f.endswith(".jpg")}
    pc_ext = f".{pc_format}"
    pcd_files = {f.split(".")[0]: f for f in os.listdir(pointcloud_dir) if f.endswith(pc_ext)}
    common_timestamps = set(image_files.keys()) & set(pcd_files.keys())

    for ts in common_timestamps:
        shutil.copy(
            os.path.join(image_dir, image_files[ts]),
            os.path.join(sync_image_dir, image_files[ts])
        )
        shutil.copy(
            os.path.join(pointcloud_dir, pcd_files[ts]),
            os.path.join(sync_pcd_dir, pcd_files[ts])
        )

def extract_data(record_file, output_dir, pc_format="pcd"):
    image_dirs = {key: os.path.join(output_dir, "images", key) for key in IMAGE_TOPICS}
    lidar_dir = os.path.join(output_dir, "pointclouds")
    imu_csv_path = os.path.join(output_dir, "imu.csv")
    imu_data = []

    with Record(record_file) as record:
        for topic, message, t in record.read_messages():
            ts = normalize_timestamp(t)

            for cam_name, cam_topic in IMAGE_TOPICS.items():
                if topic == cam_topic and hasattr(message, 'data'):
                    save_image(ts, bytes(message.data), image_dirs[cam_name])
                    break

            if topic == POINTCLOUD_TOPIC and hasattr(message, 'point'):
                points = []
                for pt in message.point:
                    x = pt.x
                    y = pt.y
                    z = pt.z
                    intensity = pt.intensity
                    ring = getattr(pt, 'ring', 0)
                    timestamp = getattr(pt, 'timestamp', 0.0)
                    points.append([x, y, z, intensity, ring, timestamp])
                save_pointcloud(ts, points, lidar_dir, pc_format)

            elif topic == IMU_TOPIC and hasattr(message, 'linear_acceleration') and hasattr(message, 'angular_velocity'):
                acc = message.linear_acceleration
                gyro = message.angular_velocity
                imu_data.append([
                    f"{ts:.9f}",
                    acc.x, acc.y, acc.z,
                    gyro.x, gyro.y, gyro.z
                ])

    save_imu_csv(imu_data, imu_csv_path)

    for cam_name, image_dir in image_dirs.items():
        sync_image_dir = os.path.join(output_dir, f"sync_images_pcd_{cam_name}")
        sync_pcd_dir = os.path.join(output_dir, f"sync_images_pcd_{cam_name}")
        copy_synced_data(image_dir, lidar_dir, sync_image_dir, sync_pcd_dir, pc_format)

    print("[Done] All data extracted and synced to", output_dir)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("record_file", help="Path to Apollo record file")
    parser.add_argument("output_dir", help="Directory to save extracted data")
    parser.add_argument("--pc_format", choices=["pcd", "bin"], default="pcd",
                        help="Output format for point clouds: 'pcd' or 'bin'")
    args = parser.parse_args()

    extract_data(args.record_file, args.output_dir, args.pc_format)
