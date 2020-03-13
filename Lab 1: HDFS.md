## 0. Connect to edge node

Comment `ssh l.wu-dsti@edge-1.au.adaltas.cloud`, and key in the password

`pwd`, print working directory, to find where you are now


## 1. Download the most frequently downloaded e-book (in Plain Text UTF-8) from Project Gutenberg.

`wget http://www.gutenberg.org/files/1342/1342-0.txt`

Check the file's existence by `ls`

In order to read it easily in the future, 
rename the file as **Pride_and_Prejudict.txt** by `mv 1342-0.txt pride_and_prejudice.txt`

## 2. Create a directory called **raw** inside your HDFS home directory.

`hdfs dfs -mkdir raw`

## 3. Put the downloaded e-book inside this directory.

`hdfs dfs -put pride_and_prejudice.txt raw` as same as `hdfs dfs -copyFromLocal pride_and_prejudice.txt raw`

Check the file's existence in raw directory by `hdfs dfs -ls raw`

## 4. Create a copy directly inside your HDFS home directory.

`hdfs dfs -put pride_and_prejudice.txt raw/my_ebook.txt`

## 5. Rename the copy inside your HDFS home directory to **input.txt**.

`hdfs dfs -mv raw/my_ebook.txt raw/input.txt`

## 6. Read directly from HDFS the **input.txt** file

There are two ways to read the file
One is showing the full file content by `hdfs dfs -cat raw/input.txt`
However, sometimes the storage of the file is too large make it both time and storage consuming
That's why often people use the second comment, `hdfs dfs -head raw/input.txt`, shows the title of the file

Use `du -sh pride_and_prejudice.txt` to check how big is the file first, then decide which way you preper

## 7. Remove this file (careful with this command).

Comment `rm pride_and_prejudice.txt` for remove the file in home NOT raw directory

## 8. List your HDFS home directory.

Comment `ls`, you will see there is nothing in home directory now

## 9. Retrieve the fule from the **raw** directory from HDFS to the local filesystem and rename it **local.txt**.

Always call a help with `hdfs dfs -help` when you don't know what to comment. 
Here we use `hdfs dfs -help | grep` to filter only grep function in HDFS langauge

So we knows `hdfs dfs -get raw/input.txt` is the awnser. 
Then rename it by `mv input.txt local.txt`
