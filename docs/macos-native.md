# macOS native (experimental)

To run on Apple Silicon with MPS acceleration, the code and dependencies need to run **natively**, not inside Docker. For now there is no Mac build maintained, and support is very experimental. Below are the commands tested successfully on one Macbook Pro M1. Since this is still early, the commands may need to be modified on other machines. If you give it a try, any feedback or improvements are very welcome.

## Build commands

Use cmake 3.27.9, which can be downloaded from https://cmake.org/files/v3.27/cmake-3.27.9-macos-universal.dmg and installed manually.

Make sure the correct cmake is used:
```bash
alias cmake=/Applications/CMake.app/Contents/bin/cmake
cmake --version
# Should show 3.27.9
```

```bash

python3.12 -m venv .venv-hloc

source .venv-hloc/bin/activate

pip install --upgrade pip setuptools wheel
pip install "pybind11[global]==3.0.1"

cd reconstruction-server

mkdir 3rdparty
cd 3rdparty

git clone --depth 1 --branch v0.6.0 https://github.com/google/glog.git
cd glog
cmake -S . -B build -G "Unix Makefiles"
sudo cmake --build build --target install
cd ..

git clone --recursive https://ceres-solver.googlesource.com/ceres-solver
brew install eigen@3
brew link --force eigen@3
cd ceres-solver
rm BUILD
mkdir build
cd build
cmake .. -GNinja -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF
ninja -j4
sudo ninja install
cd ../..

git clone --depth 1 --branch v2.5 https://github.com/cvg/pyceres.git
cd pyceres
pip install -e .
cd ..

git clone https://github.com/colmap/colmap.git
cd colmap
git checkout 20b2777186654f20745f6c57974924d145bbbd6f
mkdir build_mac && cd build_mac
brew install metis suitesparse qt glew libomp sqlite3
brew link --force libomp
brew unlink qt && brew link --force qt
cmake .. -GNinja -DCUDA_ENABLED=OFF -DCGAL_ENABLED=OFF -DGUI_ENABLED=OFF
ninja -j4
sudo ninja install
cd ..
pip install -e .
cd ..

# In Hierarchical-Localization clone (right now used local folder in my exocortex)
# Normally:
# git clone --recursive https://github.com/aukilabs/Hierarchical-Localization
# cd Hierarchical-Localization; \
# git checkout --recurse-submodules 87b266cdb3a894455a9c889276f4e5d5913eb0eb
pip install torch==2.9.1 torchvision==0.24.1 torchaudio==2.9.1
pip install -e . --config-settings editable_mode=compat
cd ../..

cd reconstruction-server
cd 3rdparty

git clone --depth 1 https://github.com/google/draco.git
cd draco
mkdir build
cd build
cmake .. -GNinja -DCMAKE_INSTALL_PREFIX=/usr/local
ninja -j4
sudo ninja install
cd ..

brew link --force sqlite3
cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON
# Manually edit the FindDependencies.cmake in colmap to comment out the find_package(OpenGL ...) line
cmake --build build

# Rust build natively (not in docker)
# Needs rust v1.89
rustup install 1.89.0
cd reconstruction-server/server/rust
cargo +1.89.0 build --release -p bin
cp target/release/compute-node ../../compute-node
cd ../..

# Make sure you have environment variables set up for the node
# Run server
./compute-node

```

## Smoke tests

From **reconstruction-server** repo root, venv active:

```bash
python tests/test_pyceres_smoke.py
python tests/test_pycolmap_smoke.py
python tests/test_hloc_smoke.py
python tests/test_hloc_feature_match_dmt_smoke.py --frames-dir tests/data/test_frames
```