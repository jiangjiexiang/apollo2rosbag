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
   
2. Create and activate the conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate record2bag
   
## Usage
1. Open apollo_to_rosbag_ringtime_nowarn.py in a text editor
2. Modify the topic list as needed
3. Save and run the conversion script
   ```bash
   source ~/ws_livox/devel/setup.bash
   
   python apollo_to_rosbag_ringtime_nowarn.py <input.record> <output.bag>

   python record2bag_mid360.py record-0726/20250726170621.record.00001 record0
