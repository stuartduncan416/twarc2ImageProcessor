import argparse
import json
import csv
import dateutil.parser
import os
from urllib.parse import urlparse
import requests
import time
import pandas as pd
import shutil

def processJson(jsonFile,outputFile,verbose):

    jsonData = []

    # load json file from twarc

    with open(jsonFile) as f:
        for line in f :
            jsonData.append(json.loads(line))

    count = 1

    # create list of csv header row values
    csvHeader = ['id','userid', 'username', 'createdDate', 'conversationId', \
        'tweetText', 'retweet', 'reply', 'like','quotes',"isRetweet","stringImageUrls"]

    with open(outputFile, 'w', encoding='UTF8', newline='') as f:

        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)

        # write the header row to the csv file
        writer.writerow(csvHeader)

        for jsonObj in jsonData:

            # create three lists containing additional user information, tweet and image information
            # this additional data is contained in the includes section of the twarc json output

            userList = []
            tweetList = []
            imageList = []

            if "includes" in jsonObj:

                if "media" in jsonObj["includes"]:
                    for image in jsonObj["includes"]["media"]:
                        if(image["type"] == "photo"):
                            imageList.append({"mediaKey":image["media_key"], "imageUrl":image["url"], "type":image["type"]})
                        if(image["type"] == "animated_gif"):
                            imageList.append({"mediaKey":image["media_key"], "imageUrl":image["preview_image_url"],"type":image["type"]})

                if "users" in jsonObj["includes"]:
                    for user in jsonObj["includes"]["users"]:
                        userList.append({"userId":user["id"], "username":user["username"]})

                if "tweets" in jsonObj["includes"]:
                    for tweetInclude in jsonObj["includes"]["tweets"]:
                        tweetList.append({"tweetId":tweetInclude["id"], "fullText":tweetInclude["text"]})

            # read the tweet objects from the twarc json objects

            for tweet in jsonObj["data"]:

                # set default values for the variables being written to the CSV
                id = 0
                tweetText = ""
                username = ""
                userid = 0
                newDate = "1900-01-01 00:00:00"
                retweets = -100
                replys = -100
                likes = -100
                quotes = -100
                conversationId = 0
                isRetweet = False
                stringImageUrls = ""

                count = count + 1

                if "id" in tweet:
                    id = tweet["id"]

                if "text" in tweet:
                    tweetText = tweet["text"]

                if "author_id" in tweet:
                    # look up full username in our username list from the includes section of the json file
                    # otherwise you are left with only the numeric user id
                    searchUser = next((item for item in userList if item["userId"] == tweet["author_id"]), None)
                    username = searchUser["username"]
                    userid = tweet["author_id"]

                if "created_at" in tweet:
                    oldDate = dateutil.parser.parse(tweet["created_at"])
                    newDate = (oldDate.strftime("%Y-%m-%d %H:%M:%S"))

                if "public_metrics" in tweet :
                    retweets = tweet["public_metrics"]["retweet_count"]
                    replys = tweet["public_metrics"]["reply_count"]
                    likes = tweet["public_metrics"]["like_count"]
                    quotes = tweet["public_metrics"]["quote_count"]

                if "conversation_id" in tweet:
                    conversationId = tweet["conversation_id"]

                # if the tweet object contains a referenced tweet object
                # check to see if there is retweet information and if there is
                # get the full text of the original tweet, otherwise the tweet text is truncated

                if "referenced_tweets" in tweet:

                    for refTweet in tweet["referenced_tweets"]:

                        if refTweet["type"] == "retweeted":
                            isRetweet = True
                            searchTweet = next((item for item in tweetList if item["tweetId"] == refTweet["id"]), None)
                            tweetText = searchTweet["fullText"]
                        else:
                            isRetweet = False

                urlList = []

                # if the tweet object has an attachment check to make sure that it is a photo, and append the
                # image url to list, and when done make a pipe (|) seperated string of photo urls, for writing to the CSV file

                if "attachments" in tweet:
                    if "media_keys" in tweet["attachments"]:
                        for key in tweet["attachments"]["media_keys"]:
                            imageSearch =  next((item for item in imageList if item["mediaKey"] == key), None)

                            # leave animated gifs out for now
                            if(imageSearch != None):
                                if(imageSearch["type"] == "photo"):
                                    urlList.append(imageSearch["imageUrl"])

                # create pipe seperated string of image urls

                if(len(urlList) > 1):
                    stringImageUrls = "|".join(urlList)
                elif(len(urlList) == 1):
                    stringImageUrls = urlList[0]
                else:
                    stringImageUrls = ""

                # if verbose is set to True print individual tweet data while processing

                if(verbose):
                    print("==================== Tweet {} ========================".format(count))
                    print("Tweet ID: {} User ID: {} Username: {} Date: {} Tweet text: {}\n".format(id, userid,username,newDate,tweetText))

                # create a list of values for the csv writer
                tweetData = [id, userid, username, newDate,conversationId, tweetText, retweets, replys, likes, quotes,isRetweet,stringImageUrls]
                writer.writerow(tweetData)

        print("Processed {} tweets. Output written to: {}".format(count,outputFile))

def imageExtractor(outputFile):

    # read the csv into a pandas dataframe
    tweetsDf = pd.read_csv (outputFile)

    # create a new dataframe which will put each url image url in its own row
    urlsDf = pd.DataFrame(columns=["id","imageUrl"])
    newfileName = "images_{}".format(outputFile)

    urlsIndex = 0

    for index, row in tweetsDf.iterrows():

        if(row["stringImageUrls"] != ""):

            # split the string of urls if there is more than one url
            urlList = str(row["stringImageUrls"]).split("|")

            urlCount = 0

            for url in urlList:

                if url != "nan":

                    # the url of each photo url has index appended to the end to create unique ids for tweets with more than one image
                    newId = "{}_{}".format(row["id"], urlCount)
                    urlsDf.loc[urlsIndex] = [newId,url]
                    urlsIndex = urlsIndex + 1
                    urlCount = urlCount + 1

    # write the pandas dataframe to CSV
    # probably could have left this as pandas dafaframe but need the CSV for another purpose
    urlsDf.to_csv(newfileName, index=False)

    print("Extracted {} images. Output written to: {}".format(urlsIndex,newfileName))

    return(newfileName)

def imageDownloader(outputFile,verbose,nosleep):

    imagesDf = pd.read_csv (outputFile)

    sleepCount = 0
    totalImages = 0

    totalRows = len(imagesDf)

    if not verbose:
        print("Downloading images .",end='', flush=True)

    # go through the dataframe and download the images to current directory

    for index, row in imagesDf.iterrows():

        if not verbose:
            print('.', end='', flush=True)

        r = requests.get(row["imageUrl"], stream = True)
        path = urlparse(row["imageUrl"]).path
        ext = os.path.splitext(path)[1]

        filename = "{}{}".format(row["id"], ext)

        if r.status_code == 200:
            r.raw.decode_content = True

            with open(filename,'wb') as f:
                shutil.copyfileobj(r.raw, f)

            if(verbose):
                print('Image {} of {} sucessfully downloaded: {} '.format(index+1,totalRows,filename))

            totalImages = totalImages + 1
        else:
            if(verbose):
                print('Image {} of {} couldn\'t be retreived. Url : {}'.format(index+1,totalRows,row["imageUrl"]))

        # added a pause every 100 images just in case there are rate limits on download images from Twitter

        if(sleepCount == 100):

            if not nosleep:
                if(verbose):
                    print("Sleeping for 10 seconds")

                time.sleep(10)

            sleepCount = 0

        sleepCount = sleepCount + 1

    print("\nDownloaded {} images".format(totalImages))

def main():

    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--json")
    parser.add_argument("-o", "--output")
    parser.add_argument("-v", "--verbose",action='store_true')
    parser.add_argument("-d", "--download",action='store_true')
    parser.add_argument("-nosleep", "--nosleep",action='store_true')

    args = parser.parse_args()

    if args.json:
        jsonFile = args.json
    else:
        print("Please include a json file using the -j <FILE> command line arguument")

    if args.output:
        outputFile = args.output
    else:
        print("Please include a CSV output file using the -o <FILE> command line arguument")

    # if both command line arguments are present process JSON file from twarc
    if args.json and args.output:
        processJson(jsonFile,outputFile,args.verbose)

    # download the images if the command line argument is present
    if args.download:
        imageResultsFilename = imageExtractor(outputFile)
        imageDownloader(imageResultsFilename,args.verbose,args.nosleep)

if __name__ == "__main__":
    main()
