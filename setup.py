import setuptools

setuptools.setup(
    name='line-protocol-cache',
    version='0.1',
    author='XuZhen86',
    url='https://github.com/XuZhen86/LineProtocolCache',
    packages=setuptools.find_packages(),
    python_requires='>=3.11,<4',
    install_requires=[
        'absl-py>=2.1.0,<3',
        'aiosqlite>=0.19.0,<1',
        'influxdb-client>=1.39.0,<2',
        'jsonschema>=4.23.0,<5',
        'tenacity>=8.2.3,<9',
    ],
    entry_points={
        'console_scripts': [
            'line-protocol-cache-uploader = line_protocol_cache.lineprotocolcacheuploader:app_run_main',
            'bucket-migration-helper = bucket_migration_helper.main:app_run_main',
        ],
    },
)
