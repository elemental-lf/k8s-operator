from setuptools import setup, find_packages

setup(
    name='side8.k8s.operator',
    version='0.1',
    description='Kubernetes operator shim',
    url='https://github.com/side8/k8s-operator',
    author='Tom van Neerijnen',
    author_email='tom@tomvn.com',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=['kubernetes==9.0.0', 'ruamel.yaml>0.15,<0.16', 'aiojobs==0.2.2', 'async-timeout==3.0.1'],
    keywords='cli kubernetes operator',
    license='Apache',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ],
    entry_points={
        'console_scripts': [
            'side8-k8s-operator=side8.k8s.operator:main',
        ],
    },
)
