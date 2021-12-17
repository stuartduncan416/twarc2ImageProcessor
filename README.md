# twarc2ImageProcessor

Building off of the [twarc2ToCSV](https://github.com/stuartduncan416/twarc2ToCSV) Python script, this script converts twarc2 JSON result output to CSV, and includes tweet image information in that CSV. Optionally this script can also download those images to your local computer. 

The outputted CSV file contains columns for tweet id, user id, username, created date (YYYY-MM-DD HH:MM:SS format), twitter conversation id, full tweet text, amount of retweets, amount of replies, amount of likes, amount of quotes, a boolean flag indicating if the tweet is a retweet, and pipe seperated string of tweet image urls. An example of the result file is included in the repository, as is a JSON file output from twarc2 for testing. 

As indicated this script can also optionally download the tweet images found in the tweet result set to your local computer. 

This script uses several command line arguments as follows:

-j <FILE> : the twarc2 json file you would like to process
-o <FILE> : the csv file you would like the results to be outputted to
-v : an optional flag that will provide more verbose ouput as the script runs
-d : an optional flag that will download the images from your result set to the directory the script is run in
-nosleep : an optional flag that will bypass a ten second sleep function that occurs every 100 images processed
  
Here is a sample command to run the script:
 
python twarcImages.py -j snow.jsonl -o snow.csv -d -v
  
This command would process the snow.jsonl file, output the results to snow.csv, download the images to the local directory the script is run from and would show the verbose ouputs will the script runs. 

When downloading images, the script also creates a second csv file with the image results on individual lines. 
