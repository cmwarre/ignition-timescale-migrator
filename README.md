
To Setup

    virtualenv env --python=$(which python3)
    source env/bin/activate
    pip install -r requirements.txt
    


Usage:

    python main.py --host old_database -u user -p password -d database -b exportdir/ -t 2 export
    python main.py --host new_database -u user -p password -d database -b exportdir/ -t 2 import
    
    