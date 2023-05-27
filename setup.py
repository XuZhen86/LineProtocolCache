import setuptools

setuptools.setup(
    name='line-protocol-cache',
    version='0.1',
    author='XuZhen86',
    url='https://github.com/XuZhen86/LineProtocolCache',
    packages=setuptools.find_packages(),
    python_requires='>=3.11,<3.12',
    install_requires=[
        'absl-py>=1.3.0,<2',
        'aiosqlite>=0.18.0,<1',
        'influxdb-client>=1.35.0,<2',
    ],
    entry_points={
        'console_scripts': ['line-protocol-cache-consumer = line_protocol_cache.main:main',],
    },
)
