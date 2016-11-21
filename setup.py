from setuptools import setup, find_packages

setup(name='datum',
      version='0.1',
      description='Simple spatial data abstraction.',
      url='http://github.com/cityofphiladelphia/datum/',
      author='City of Philadelphia',
      author_email='maps@phila.gov',
      license='MIT',
      packages=find_packages(),
      install_requires=['six==1.10.0'],
      extras_require={
        'oracle_stgeom': ['cx-Oracle==5.2.1', 'pyproj==1.9.5.1'],
        'postgis': ['psycopg2==2.6.1'],
      },
      zip_safe=False)
