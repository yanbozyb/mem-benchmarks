#!/bin/bash

# Install python modules that dlrm requires
pip install numpy torch tqdm scikit-learn tensorboard onnx mlperf_logging

# Download the dataset (about 17GB after unzip)
if [ -f "./dataset/train.txt" ]; then
    echo "Use existing ./dataset/train.txt"
else
    echo "Downloading and unzipping dataset..."
    mkdir -p dataset
    cd dataset
    wget https://go.criteo.net/criteo-research-kaggle-display-advertising-challenge-dataset.tar.gz
    tar zxvf ./criteo-research-kaggle-display-advertising-challenge-dataset.tar.gz
    echo "Finished unzipping dataset..."
fi
DATASET="`pwd`/dataset/train.txt"

PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/dlrm

./bench/dlrm_s_criteo_kaggle.sh --raw-data-file=$DATASET