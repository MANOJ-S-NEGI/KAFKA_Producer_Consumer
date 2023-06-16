from typing import List
from setuptools import setup, find_packages

HYPEN_E_DOT = '-e .'

def get_requirements_list():
    new_list = []
    with open("requirements.txt") as requirements:
        requirement_lists = requirements.readlines()
        for requirement in requirement_lists:
            new_list.append(requirement.replace("\n", ""))
            if HYPEN_E_DOT in new_list:
                new_list.remove(HYPEN_E_DOT)
    return new_list

setup(
    name='pythonProject_senseor_fault',
    version='0.0.1',
    author='Msn21',
    description='senseor_fault',
    packages=find_packages(),
    install_requires=get_requirements_list()
)

