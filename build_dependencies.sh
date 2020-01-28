#! /usr/bin/env bash

# check to see if pipenv is installed
# shellcheck disable=SC2230
if [ -x "$(which pipenv)" ]
then
      # check that Pipfile.lock exists in root directory
      if [ ! -e Pipfile.lock ]
      then
        echo 'ERROR - cannot find Pipfile.lock'
        exit 1
      fi

      # use Pipenv to create a requirement.txt file
      echo '.... creating requirement.txt file from Pipfile.lock'
      pipenv lock -r > requirement.txt

      # install packages to a temporary diretory and zip it
      touch requirement.txt   # safegurad if there're no packages to be installed
      pip3 install -r requirement.txt  --target ./packages

      # check to see if there are any external dependencies
      # if not then create an empty file to seed zip with
      # shellcheck disable=SC1073
      if [ -z "$(ls -A packages)" ]
      then
        touch packages/empty.txt
      fi

      # zip dependencies
      if [ ! -d  packages ]
      then
        echo 'ERROR - pip failed to import dependencies'
      fi

      cd packages
      zip -9mrv packages.zip .
      mv packages.zip ..
      cd ..

      # remove temporary directory and requirement.txt
      rm -rf packages
      rm requirements.txt

      # add local modules
      echo ' ... adding all modules from local utils package '
      zip -ru9 packages.zip dependencies -x dependencies/_pycache_/\*

      exit 0
else
  # shellcheck disable=SC2006
  echo "ERROR - pipenv is not installed --> run `pip3 install pipenv` to load pipenv into global site packages or install via system package manager"
  exit 1
fi





