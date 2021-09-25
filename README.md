# Using Prefect

This is basically a diary of getting started with prefect and prefect cloud.

I skipped over Airflow entirely, but I need to learn something and prefect seems interesting and it works with Dask+Coiled which is really cool. I'm pretty comfortable with python and stuff, but unlike so much else I am learning, I am really starting at 0 here. Cool.

The long term goal is to replace a very strange workflow I have:

- A lambda function hits an API once an hour, and stores some documents to mongoDB. Why mongoDB? Because I needed a cheap database that can enforce unique constraints. The API doesn't really have a highwater mark, so need to handle duplicates.
- A google cloud function runs one a day, writing new data to bigQuery. Because the implementation was very simple, I don't have a highwater mark here either, and I do some clumsy pandas stuff to avoid inserting duplicates.
- A bigquery job is triggered about an hour after that, to calculate some intermediate outputs that is used in a dashboard.

## Signing up and getting started

1. The password policy is almost good. But I don't like hard requirements, entropy counts. Cool that it uses okta though.

2. Where is my API key? I'm just guessing I need an API key and it turns out I'm right. With Coiled, the first thing you see is an API key and some commands. I don't like having to look for (and even generate) this stuff. Found it though, and created a config file. Cool that it follows a common pattern - I'm used to this from AWS and others.

3. Why are there several similar but different "hello world" examples? It turns out there is a reason, you need to register a flow and then run it, but I'm new to all this so... Anyways, I figured it out. Sticking to running from the dashboard from now. The "quick run" button is sweet.

4. Yay I copy-pasted code and it ran! Granted I needed a few tries, but I did something and something happened and there is a green color on the dashboard. Wonder what it was I did though.

5. WTF why am I getting a deprecated warning? I haven't written a single line of code that hasn't been copy-pasted from the website. Turns out the config file is now supposed to say `api_key = "XXX"` instead of `api_token = "XXX"`. Kind of hard to spot that one, and I seem to be unable to find where I copy-pasted that from. All I can find now is instructions that say api_key.

6. Damn, executors and agents are confusing.

7. Why aren't there more complex examples? Documentation is OK, but it never shows the whole picture.

8. please please tell me I don't need an agent running 24/7 just because I want to trigger a job once a day. Isn't that what prefect cloud was supposed to do? What does actually Prefect cloud do? Is is just a dashboard? What am I missing?

9. Where is coiled? Seriously they did a whole webinar about it and I can't find a single mention of coiled on the prefect site and only a symbolic mention of prefect on the coiled website. Nice partnership. Resorting to googling, seems there is a github issue that explains the basics. Sorry, but github issues != documentation. That said though, the solution made sense and perhaps I should have figured this out myself.

10. How is this stuff handled in production? In a CI/CD environment? If I'm testing some changes to a flow and I want to push it to production? Is it normal to register the flow in the same file as the flow is defined or is that just a simplification for the hello-world example?

## Code beyond copy-paste

My first goal is to get this to run on coiled. I already have Coiled installed, so I need to get the two talking. Experimenting with ECS failed (I don't know that much about ECS and the documentation assumed I did).

I am able to do a run on coiled by using a DaskExecutor, passing an actual Coiled cluster object (`coiled.Cluster`, not a string or anything) as cluster class.

I am using a LocalRun run-config for this, and starting a local agent.

This is all simple enough, but I need to register an environment in coiled that has prefect. While that's really a simple task and I have done it a couple of times before, I do it rarely enough to have forgotten each time. This is cool though. Basically it seems to be working I just need to get my ducks in a row.

It's amazing what copy-paste can do. `create_coiled_env.py` will create an env on coiled, using the `environment.yaml` file we have here locally.


## Round 2

Azure didn't work out very well as a agent host (some finnicking I didn't want to spend time on), so I'm back on AWS. Still using coiled to do all the heavy lifting though.

One problem I faced was that I'm using bigquery, which needs a json key file for authentication. Since I'm running on coiled i needed to get the keyfile to the servers, which is normally handeled by the `client.upload_file("my_script.py")` method, but since I'm launching the entire thing into coiled I don't have access to the `client` object. There are a number of solutions, the one I went with was to store the key-file as a json object in the AWS secrets manager.

Some tips so far:
1. debug locally using the flow.run() command rather than registering the flow and running. At some point you have to register and debug the entire run, but because we are invoking coiled the way we do we can simply change the flow run configurations and run locally instead.

2. Everything inside the `with Flow(...) as flow:` statement except the task function calls is run when registering the function, so make sure the stuff in the flow is quick, easy and harmless to run. Among other things, it forced me to create a mini-task simply for updating a key-vault timestamp (high water mark) so that I wouldn't ruin state simply by registering the function.
