from setuptools import setup
import os


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [dirpath
            for dirpath, dirnames, filenames in os.walk(package)
            if os.path.exists(os.path.join(dirpath, '__init__.py'))]


setup(name='datum',
      version='0.1',
      description='Simple spatial data abstraction.',
      url='http://github.com/cityofphiladelphia/datum/',
      author='City of Philadelphia',
      author_email='maps@phila.gov',
      license='MIT',
      packages=get_packages('datum'),
      install_requires=['six==1.10.0', 'click==6.6'],
      extras_require={
        'oracle_stgeom': ['cx-Oracle==5.2.1', 'pyproj==1.9.5.1', 'shapely==1.5.17'],
        'postgis': ['psycopg2==2.6.1'],
      },
      entry_points={
        'console_scripts': ['datum=datum.cli:cli']
      },
      # entry_points={'console_scripts': ['ais=ais:manager.run']},
      zip_safe=False)
