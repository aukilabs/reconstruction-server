# Reconstruction Node
This repository has the code and algorithms for the reconstruction node, part of the Auki Network. The reconstruction node is used when setting up domains.

The node processes scans from the Domain Management Tool (DMT) app ([App Store](https://apps.apple.com/app/domain-management-tool/id6499270503) 🔗). A custom structure-from-motion (SfM) pipeline improves accuracy and creates 3D representations such as point clouds, bounding boxes, and gaussian splats.

For more information on the reconstruction service, please refer to our [whitepaper](https://auki.gitbook.io/whitepaper/technical-overview/the-reconstruction-service).

## Documentation
- [Minimum Requirements](docs/minimum-requirements.md)
- [Deployment](docs/deployment.md)

## Acknowledgments

This project builds upon the work of many excellent open-source projects, including
[hloc](https://github.com/cvg/Hierarchical-Localization), [COLMAP](https://colmap.github.io), [Ceres Solver](http://ceres-solver.org),
[Eigen](https://eigen.tuxfamily.org), [PyTorch](https://pytorch.org),
[OpenCV](https://opencv.org), [Open3D](https://www.open3d.org), and others.

We thank their authors and contributors for making this work possible.  
Please note that all third-party code and libraries are subject to their respective licenses, copyrights, and trademarks.
We are not affiliated with, endorsed by, or sponsored by any of the projects or organizations mentioned above.