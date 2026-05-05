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

Install Livox message packages separately if you use `record2bag_mid360.py`.
For Livox ROS Driver 2, source the workspace that provides
`livox_ros_driver2/CustomMsg`.
   
## Usage

Source ROS and the Livox message workspace before running conversion:

```bash
source /opt/ros/noetic/setup.bash
source ~/ws_livox/devel/setup.bash
conda activate record2bag
```

### List Apollo record topics

Use the helper script to inspect a record file or a directory of record splits:

```bash
python list_apollo_topics.py record410
python list_apollo_topics.py record410 --per-file
```

If a record index is broken or incomplete, scan messages directly:

```bash
python list_apollo_topics.py record410 --scan
python list_apollo_topics.py record410 --scan --limit 50000
```

### Convert Mid360 Apollo records

Convert one Apollo record file:

```bash
python record2bag_mid360.py record410/20260409172152.record.00000 rosbag410/0.bag --point_unit m
```

Convert every file in a directory. Output bags are named by sorted input order:
`0.bag`, `1.bag`, `2.bag`, and so on.

```bash
python record2bag_mid360.py record410 rosbag410 --jobs 8 --point_unit m
```

Use Livox ROS Driver 2 message types:

```bash
python record2bag_mid360.py record410 rosbag410 --jobs 8 --use_driver2 --point_unit m
```

Distance unit options:

```bash
--point_unit auto  # Auto-detect input point unit.
--point_unit m     # Input point coordinates are already meters.
--point_unit mm    # Input point coordinates are millimeters.
```

Override Apollo input topics if needed:

```bash
python record2bag_mid360.py record410 rosbag410 \
  --imu_topic /apollo/sensor/mid_360/Imu \
  --pc_topic /apollo/sensor/mid_360/PointCloud2 \
  --jobs 8 \
  --point_unit m
```

### Convert other Apollo records

For the camera/Hesai converter, edit the topic constants in
`apollo_to_rosbag_ringtime_nowarn.py`, then run:

```bash
python apollo_to_rosbag_ringtime_nowarn.py <input.record> <output.bag>
```

### Play records

Play an Apollo Cyber record with Apollo tools:

```bash
cyber_recorder play -f record410/20260409172152.record.00000
cyber_recorder info record410/20260409172152.record.00000
```

Play a converted ROS bag:

```bash
rosbag play rosbag410/0.bag
```
