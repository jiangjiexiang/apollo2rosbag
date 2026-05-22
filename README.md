# Apollo Cyber Record Tools

A toolkit for processing Apollo Cyber RT record files, including:
- Conversion from Apollo Cyber Record to ROS bag format
- Sensor data extraction from Apollo Cyber Records

## Features

- Convert Apollo Cyber RT `.record` files to ROS `.bag` format
- Extract specific sensor data from recorded sessions
- Maintain message timing and structure during conversion

## Prerequisites

- Anaconda or Miniconda
- ROS (for working with the output bag files)

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/SimonovBlonsky/apollo2rosbag.git
   cd apollo2rosbag
   ```

2. Create and activate the conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate record2bag
   ```

### NVIDIA Jetson Orin notes

The `environment.yml` is intentionally not a full `conda env export` lockfile.
Jetson Orin uses `linux-aarch64`, so x86_64 build pins such as `linux-64` or
`h5eee18b_0` will not install.

Install ROS and message packages from the system ROS installation, not from
pip/conda:

```bash
sudo apt-get install ros-noetic-ros-base ros-noetic-cv-bridge
source /opt/ros/noetic/setup.bash
conda activate record2bag
```

If your ROS packages are not visible inside Conda, keep the ROS Python path in
the environment before running the scripts:

```bash
export PYTHONPATH=/opt/ros/noetic/lib/python3/dist-packages:$PYTHONPATH
```

Install Livox message packages separately if you use `record2bag_all.py` or
`record2bag_mid360.py`. For Livox ROS Driver 2, source the workspace that
provides `livox_ros_driver2/CustomMsg`.

## Usage

Run conversions with ROS and Livox sourced:

```bash
source /opt/ros/noetic/setup.bash
source ~/ws_livox/devel/setup.bash
conda activate record2bag
```

### 查看 record 里有哪些话题

```bash
python list_apollo_topics.py 0522/20260522111222.record.00000
python list_apollo_topics.py 0522                  # 整个目录
```

### 转换（推荐：全话题）

`record2bag_all.py`：有数据的话题都转；Mid360 点云/IMU 走 Livox 格式，其余话题名与 Apollo 一致。

```bash
# 单个文件
python record2bag_all.py 0522/20260522111222.record.00000 out.bag

# 整个文件夹 -> rosbag_out/0.bag, 1.bag, ...（默认 4 进程并行）
python record2bag_all.py 0522 rosbag_out
python record2bag_all.py 0522 rosbag_out --jobs 8
```

Mid360 话题映射：

| Apollo | ROS |
|---|---|
| `/apollo/sensor/mid_360/PointCloud2` | `/livox/lidar` (`livox_ros_driver/CustomMsg`) |
| `/apollo/sensor/mid_360/Imu` | `/livox/imu` (`sensor_msgs/Imu`) |

常用可选参数：

```bash
--use_driver2          # 使用 livox_ros_driver2
--point_unit mm        # 点云坐标是毫米时再指定（默认已是 m）
```

### 只转 Mid360（Livox）

不需要定位/GNSS 等其它话题时用：

```bash
python record2bag_mid360.py 0522/20260522111222.record.00000 out.bag
python record2bag_mid360.py 0522 rosbag_out --jobs 8
```

### 播放

```bash
rosbag play out.bag
rosbag play rosbag_out/0.bag
```
