from setuptools import setup, find_packages

setup(
    name='team7_typer',
    version='0.1.0',
        packages=find_packages(),
    include_package_data=True,
    package_data={
        "": [".env"],
    },
    py_modules=['typer_main'],
    install_requires=[
        'typer',
        'boto3',
        'python-dotenv',
        'pandas',
        'python-dotenv',
        'bcrypt'

                ],
entry_points={
    'console_scripts': [
        'typer_app=team7_typer.typer_main:app',
    ],
}
)
