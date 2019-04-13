from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='courtpy',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="CourtPy offers flexible, accessible tools for parsing and analysis of court opinions",
    author="Corey Rayburn Yung",
    author_email='coreyyung@ku.edu',
    url='https://github.com/with_precedent/courtpy',
    packages=['courtpy'],
    entry_points={
        'console_scripts': [
            'courtpy=courtpy.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='courtpy',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
