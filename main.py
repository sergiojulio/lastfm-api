#!/usr/bin/env python
import requests


def main(key):
    x = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + key + '&format=json')
    print(x.text)
    

if __name__ == '__main__':
    main()