import os
import sys
import struct
import rosbag
import rospy
from sensor_msgs.msg import CompressedImage, PointCloud2, Imu, PointField
import sensor_msgs.point_cloud2 as pcl2
from std_msgs.msg import Header
from cv_bridge import CvBridge
from cyber_record.record import Record
import cv2
import numpy as np

bridge = CvBridge()


def normalize_timestamp(timestamp):
    return max(0, timestamp)


def extract_data(record_file):
    IMAGE_TOPIC = "/apollo/sensor/drivers/camera/surround_camera/front/compress"
    IMU_TOPIC = "/vts/drivers/ins570d/checked_imu"
    POINTCLOUD_TOPIC = "/vts/sensor/hesai40/compensated/PointCloud2"

    image_data = []
    pointcloud_data = []
    imu_data = []

    with Record(record_file) as record:
        for topic, message, t in record.read_messages():
            normalized_t = normalize_timestamp(t)

            if topic == IMAGE_TOPIC:
                try:
                    if hasattr(message, 'data') and hasattr(message, 'format'):
                        image_data.append({
                            'timestamp': normalized_t,
                            'data': bytes(message.data),
                            'format': message.format.lower(),
                            'original_timestamp': t
                        })
                except Exception as e:
                    print(f'Image error: {e}')

            elif topic == POINTCLOUD_TOPIC:
                # try:
                    if hasattr(message, 'point'):
                        points = []
                        for point in message.point:
                            points.append([
                                point.x, point.y, point.z, point.intensity,
                                getattr(point, 'timestamp', 0.0),
                                getattr(point, 'ring', 0)
                            ])
                        # breakpoint()
                        pointcloud_data.append({
                            'timestamp': normalized_t,
                            'points': points,
                            'original_timestamp': t
                        })
                # except Exception as e:
                    # print(f'PointCloud error: {e}')

            elif topic == IMU_TOPIC:
                try:
                    if hasattr(message, 'linear_acceleration') and hasattr(message, 'angular_velocity'):
                        imu_data.append({
                            'timestamp': normalized_t,
                            'linear_acc': [
                                message.linear_acceleration.x,
                                message.linear_acceleration.y,
                                message.linear_acceleration.z
                            ],
                            'angular_vel': [
                                message.angular_velocity.x,
                                message.angular_velocity.y,
                                message.angular_velocity.z
                            ],
                            'original_timestamp': t
                        })
                except Exception as e:
                    print(f'IMU error: {e}')

    return image_data, pointcloud_data, imu_data


def create_rosbag(output_bag, image_data, pointcloud_data, imu_data):
    with rosbag.Bag(output_bag, 'w') as bag:
        # 写入图像（CompressedImage）
        for img in image_data:
            try:
                header = Header()
                header.stamp = rospy.Time.from_sec(img['timestamp'] * 1e-9)
                header.frame_id = "camera_link"

                # 解码为 OpenCV 图像
                np_arr = np.frombuffer(img['data'], np.uint8)
                cv_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

                if cv_img is None:
                    print(f"[WARN] Failed to decode image at {img['timestamp']}")
                    continue

                # 使用 cv_bridge 转为 sensor_msgs/Image
                ros_img = bridge.cv2_to_imgmsg(cv_img, encoding="bgr8")
                ros_img.header = header

                # 写入原始 topic
                bag.write("/apollo/sensor/drivers/camera/surround_camera/front/compress", ros_img, header.stamp)
            except Exception as e:
                print(f"[ERROR] Image conversion failed at {img['timestamp']}: {e}")
                
        for pc in pointcloud_data:
            try:
                timestamp = pc['timestamp'] * 1e-9  # 秒
                header = Header()
                header.stamp = rospy.Time.from_sec(timestamp)
                header.frame_id = "velodyne"

                fields = [
                    PointField('x', 0, PointField.FLOAT32, 1),
                    PointField('y', 4, PointField.FLOAT32, 1),
                    PointField('z', 8, PointField.FLOAT32, 1),
                    PointField('intensity', 12, PointField.FLOAT32, 1),
                    PointField('t', 16, PointField.FLOAT32, 1),
                    PointField('ring', 20, PointField.UINT16, 1),
                ]

                # 构建点云数据 [(x, y, z, time_offset, ring), ...]
                processed_points = []
                for pt in pc['points']:
                    x, y, z, intensity, time_ns, ring = pt
                    time_offset_sec = float(time_ns * 1e-9) - timestamp
                    # if time_offset_sec < 0 or time_offset_sec > 0.5:
                    #     print(f"Warning: large time offset {time_offset_sec:.6f} s")
                    processed_points.append((x, y, z, intensity, time_offset_sec, int(ring)))

                ros_pc = pcl2.create_cloud(header, fields, processed_points)
                ros_pc.is_dense = True
                bag.write('/hesai/pandar', ros_pc, t=ros_pc.header.stamp)
            except Exception as e:
                print(f'PointCloud write error: {e}')

        # 写入 IMU
        for imu in imu_data:
            try:
                ros_imu = Imu()
                header = Header()
                header.stamp = rospy.Time.from_sec(imu['timestamp'] * 1e-9)
                header.frame_id = "imu_link"
                ros_imu.header = header
                ros_imu.linear_acceleration.x = imu['linear_acc'][0]
                ros_imu.linear_acceleration.y = imu['linear_acc'][1]
                ros_imu.linear_acceleration.z = imu['linear_acc'][2]
                ros_imu.angular_velocity.x = imu['angular_vel'][0]
                ros_imu.angular_velocity.y = imu['angular_vel'][1]
                ros_imu.angular_velocity.z = imu['angular_vel'][2]
                bag.write("raw_imu", ros_imu, header.stamp)
            except Exception as e:
                print(f'IMU write error: {e}')


def main():
    if len(sys.argv) != 3:
        print("Usage: python apollo_to_rosbag.py <input.record> <output.bag>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        sys.exit(1)

    rospy.init_node('apollo_to_rosbag', anonymous=True)

    print(f"Converting {input_file} to {output_file}...")

    image_data, pointcloud_data, imu_data = extract_data(input_file)

    create_rosbag(output_file, image_data, pointcloud_data, imu_data)

    print("Conversion completed successfully.")


if __name__ == '__main__':
    main()