from setuptools import setup, find_packages

setup(
    name='pykubeoperator',
    version='0.1',
    description='Minimal Kubernetes Operator SDK for Python',
    url='https://github.com/elemental-lf/pykubeoperator',
    author='Lars Fenneberg',
    author_email='lf@elemental.net',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=['kubernetes==9.0.0', 'ruamel.yaml>0.15,<0.16', 'aiojobs==0.2.2', 'async-timeout==3.0.1'],
    keywords='kubernetes operator',
    license='Apache',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ],
)
