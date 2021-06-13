import re
from pathlib import Path
from setuptools import setup, find_packages

minimum_requirements = [
    "Pillow",
    "pandas",
    "boto3",
    "matplotlib"
]

full_requirements = minimum_requirements + [
    "notebook"
]

test_requirements = minimum_requirements + [
    "pytest"
]

version_file_path = (
    Path(__file__).parent / "src/open_dataset_tools/__init__.py"
)

def find_version(version_file_path: Path):
    with version_file_path.open("r") as f:
        version_file_contents = f.read()
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", version_file_contents, re.M
    )
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup (
    version=find_version(version_file_path),
    name="aibs_open_dataset_tools",
    description=(
        "An open source package containing example code and Ipython notebooks "
        "that demonstrate how to access open datasets such as the "
        "Allen Mouse Brain Atlas or the Ivy Glioblastoma Atlas"
    ),
    author="Scott Daniel, Nicholas Mei, Wayne Wakeman",
    author_email="waynew@alleninstitute.org",
    url="https://github.com/AllenInstitute/open_dataset_tools",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=minimum_requirements,
    extras_require={
        "full": full_requirements,
        "test": test_requirements
    }
)