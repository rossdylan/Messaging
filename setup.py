from setuptools import setup
setup(name = "cshmsg",
        descripition = "Unified CSH Messaging System",
        author = "Ross Delinger",
        version = "0.1.0",
        install_requires = ["pyzmq",],
        py_modules = ['messaging',])
