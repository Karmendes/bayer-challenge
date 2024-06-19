# Verify if you are passing a name for virtualenv
if [ $# -eq 0 ]; then
  echo "Please provide a name for the virtual environment as an argument."
  exit 1
fi

# Virtual environment name
virtualenv_name=$1

# Create virtual environment
echo "Creating virtual environment $virtualenv_name ..."
virtualenv $virtualenv_name

# Activate virtual environment
echo "Activating virtual environment $virtualenv_name ..."
source "./$virtualenv_name/Scripts/activate"  # Assuming Windows system

# Create and configure .gitignore
echo "**/__pycache__" >> .gitignore
echo "$virtualenv_name" >> .gitignore
echo "Creating .gitignore..."

# Install pylint in the virtual environment
echo "Installing pylint in the virtual environment..."
pip install pylint

# Configure VSCode settings.json file
echo "Configuring VSCode settings.json..."
# Ensure directory exists
mkdir -p .vscode
# Create and write to settings.json
cat <<EOF > .vscode/settings.json
{
  "python.linting.pylintEnabled": true,
  "python.linting.enabled": true,
  "files.exclude": {
    "**/*.pyc": { "when": "$(basename).py" },
    "**/__pycache__": true,
    "**/*.pytest_cache": true
  }
}
EOF

# Create pylint configuration file
echo "Creating pylint configuration file..."
pylint --generate-rcfile > .pylintrc

# Install pre-commit in the virtual environment
echo "Installing pre-commit in the virtual environment..."
pip install pre-commit
pre-commit install

# Add pre-commit configuration
echo "Adding pre-commit configuration..."
cat <<EOF > .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pylint
        name: pylint
        entry: pylint
        language: system
        types: [python]
        args:
          [
            "-rn", # Only display messages
            "-sn", # Don't display the score
            "--rcfile=.pylintrc", # Link to your config file
            "--load-plugins=pylint.extensions.docparams", # Load an extension
          ]
  - repo: local
    hooks:
      - id: requirements
        name: requirements
        entry: bash -c 'pip freeze > requirements.txt; git add requirements.txt'
        language: system
        pass_filenames: false
        stages: [commit]
EOF

echo "Setup completed."