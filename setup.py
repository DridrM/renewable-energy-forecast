from setuptools import setup
from setuptools import find_packages

# List the dependencies frome the requirements.txt
with open("requirements.txt") as r:
    # Read the content
    content = r.readlines()

# Store the requirements into a list
requirements = [l.strip() for l in content]

# Setup our package
setup(name = "re_forecast",
      version = "0.5",
      description = "Predict renewable energy production",
      packages = find_packages(),
      install_requires = requirements)
