from setuptools import setup

setup(name='datum',
      version='0.1',
      description='Simple spatial data abstraction.',
      url='http://github.com/cityofphiladelphia/datum/',
      author='City of Philadelphia',
      author_email='maps@phila.gov',
      license='MIT',
      packages=['datum'],
      # entry_points={'console_scripts': ['ais=ais:manager.run']},
      zip_safe=False)
