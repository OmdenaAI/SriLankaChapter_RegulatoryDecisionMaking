import os
import getpass
from dotenv import load_dotenv


# Step 1: Create or update .env file by prompting the user for their credentials using getpass
def create_or_update_env():
    env_file_path = "/content/drive/MyDrive/.env"

    # Check if the .env file already exists
    if not os.path.exists(env_file_path):
        # .env doesn't exist, so prompt user for credentials and create the file
        print("No .env file found. Creating a new one.")
        username = input("Enter your GitHub username: ")
        token = getpass.getpass("Enter your GitHub token: ")

        # Write username and token to the .env file
        with open(env_file_path, "w") as f:
            f.write(f"username={username}\n")
            f.write(f"token={token}\n")

        print(".env file created and updated with your credentials.")
    else:
        # .env file exists, ask user whether to overwrite or keep the existing file
        print(".env file already exists.")
        choice = input("Do you want to overwrite it? (y/n): ").strip().lower()

        if choice == "y":
            # If the user opts to overwrite, prompt for new credentials and overwrite the file
            username = input("Enter your GitHub username: ")
            token = getpass.getpass("Enter your GitHub token: ")

            with open(env_file_path, "w") as f:
                f.write(f"username={username}\n")
                f.write(f"token={token}\n")

            print(".env file has been overwritten with your new credentials.")
        else:
            # If the user chooses not to overwrite, simply load the existing .env file
            print("Using the existing .env file.")


# Step 2: Install python-dotenv if not already installed
try:
    import dotenv
except ImportError:
    print("python-dotenv not found. Installing...")
    os.system("pip install -q python-dotenv")


# Step 3: Load environment variables from .env file
def load_env():
    env_file_path = "/content/drive/MyDrive/.env"
    if not os.path.exists(env_file_path):
        print("Error: .env file not found!")
        return

    load_dotenv(dotenv_path=env_file_path)
    print(".env file loaded.")


# Step 4: Safeguard - Ensure .env is added to .gitignore
def ensure_gitignore():
    gitignore_path = "/content/drive/MyDrive/.gitignore"
    if not os.path.exists(gitignore_path):
        with open(gitignore_path, "w") as f:
            f.write(".env\n")
        print(".env added to .gitignore.")
    else:
        with open(gitignore_path, "a") as f:
            if ".env" not in f.read():
                f.write(".env\n")
                print(".env added to .gitignore.")
            else:
                print(".env already in .gitignore.")


# Main execution logic
if __name__ == "__main__":
    create_or_update_env()  # Create or update .env file by prompting user for credentials
    load_env()  # Load environment variables from .env file
    ensure_gitignore()  # Ensure .env is added to .gitignore

    # Access the variables (just for demonstration)
    USERNAME = os.getenv("username")
    TOKEN = os.getenv("token")
    print(f"Username: {USERNAME}")
    print(f"Token: {TOKEN}")