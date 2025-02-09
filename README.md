# bigdata-project

This repository contains the project for the Big Data course.

## Jobs
It contains 2 different jobs, both implemented in two version, one non-optimized and one optimized.
- The first job calculates the average number of TracksForArtist in all the playlists.
- The second one, given a specific song, calculates the most similar track related to that, (the song that appears more time in the same playlists of the target one), and the number of playlists that they share.

## Dataset
Download dataset:

```bash
#!/bin/bash
curl -L -o ~/Downloads/spotify-millions-playlist.zip\ https://www.kaggle.com/api/v1/datasets/download/adityak80/spotify-millions-playlist
```

## READMEs

Additional readmes:
- [Initial Setup](readmes/initial-setup.md) (instructions to setup the environment)
- [AWS CLI cheatsheet](readmes/aws-cli-cheatsheet.md) (a collection of the most common commands to use on the AWS CLI)
- [AWS Workflow](readmes/aws-workflow.md) (a vademecum of the list of things to do to setup the AWS environment and use it to deploy Spark jobs)
- [Exam Project](readmes/project.md) (instructions for the project, that is mandatory for the exam)
