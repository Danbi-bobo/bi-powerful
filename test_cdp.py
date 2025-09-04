import os
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv('CDP_PATH')
print(f'CDP_PATH: {CDP_PATH}')

if CDP_PATH and os.path.exists(CDP_PATH):
    print('✅ CDP_PATH directory exists')
    
    # Kiểm tra các thư mục con
    cdp_dir = os.path.join(CDP_PATH, 'cdp')
    entries_dir = os.path.join(CDP_PATH, 'entries')
    
    if os.path.exists(cdp_dir):
        print('✅ cdp folder found')
    else:
        print('❌ cdp folder not found')
        
    if os.path.exists(entries_dir):
        print('✅ entries folder found')  
    else:
        print('❌ entries folder not found')
        
else:
    print('❌ CDP_PATH directory does not exist or not set')
    print('Please check .env file')