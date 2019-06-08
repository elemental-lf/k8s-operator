from setuptools import setup, find_packages

setup(
    name='k8s-mini-operator',
    version='0.1.0',
    description='(Very) minimal Kubernetes Operator SDK for Python',
    url='https://github.com/elemental-lf/k8s-mini-operator',
    author='Lars Fenneberg',
    author_email='lf@elemental.net',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=['kubernetes==10.0.0', 'ruamel.yaml>0.15,<0.16'],
    keywords='kubernetes operator',
    license='Apache',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ],
)
