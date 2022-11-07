from setuptools import setup


setup(
    name='sisyphus',
    version='1.0.0',
    packages=['sisyphus', 'sisyphus.templates'],
    license='Mozilla Public License Version 2.0',
    long_description=open('README.md').read(),
    author="Jan-Thorsten Peter",
    author_email="jtpeter@apptek.com",
    url="https://github.com/rwth-i6/sisyphus",
    install_requires=['psutil', 'flask']
)
