import requests 
import json
API_KEY = "AIzaSyAjwzDisU4h7gojuXfONnB7NPnHkLws9Mw"
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}'

        response = requests.get(url)
        response.raise_for_status()
        # print(response)

        data = response.json()
        #print(json.dumps(data,indent=4))
        Channel_items = data["items"][0]
        Channel_playlistId = Channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(Channel_playlistId)
        return Channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
if __name__ == "__main__":
    get_playlist_id()
    


