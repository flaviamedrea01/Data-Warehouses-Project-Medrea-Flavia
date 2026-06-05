# Data-Warehouses-Project-Medrea-Flavia

## How to Reproduce Results

Follow these steps to set up your environment, launch the backend application, start the server interface, and perform the system validation protocol.

### Prerequisites
* Python 3.10 or higher installed.
* MongoDB Community Server running locally on its default port (`mongodb://localhost:27017`).
* MongoDB Compass (highly recommended for verifying collection layouts).

### Clone and Set Up Environment
Open your terminal and position your cursor inside the root project directory:
```bash
# Clone or navigate to the project root folder
cd dw-project

# Initialize a clean virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# On macOS / Linux:
source venv/bin/activate

# Install required system dependencies
pip install fastapi uvicorn motor httpx streamlit plotly pandas
```


To run the project after all dependencies are installed, open project -> split terminal -> first terminal: python -m uvicorn app.main:app --reload -> second terminal: cd app -> second terminal: streamlit run interface.py
