# jqueue

jqueue is a *very* simple job queue, which is intended to scale simple jobs
over networks. Its simplicity comes at the expense of various features which
are present in even very basic message-delivery systems (AMQP, STOMP, etc.):

 - It is *static* - there is no way to add jobs dynamically, since jobs are 
   given as filenames.
 - Clients cannot send messages to each other; they can only communicate with
   the server.
 - Transactions are not provided.
 - There is only one set of jobs, unlike message-delivery systems which allow
   for multiple 'channels' and the like.

The goal of `jqueue` is to make it easy to distribute simple jobs among
multiple nodes, where a *job* is a blob of data inside a file. The server
starts out with a list of files, which contain jobs, and then sends them
out to clients, who then send back a *result* (another blob of data) which
is put into a different file. Although minimalist, this system is enough
to accomplish simple batch tasks, like encoding a group of audio files or
scaling a large number of image files.

## Requirements

Currently, all that is required is Python 3.4, or Python 3.x along with the
`singledispatch` module on PyPI. Run `python3 setup.py install` to install
the `jqueue` library and `jqueue-server`.

## Running the Server

The server is called `jqueue-server`, and needs to be run with a list of
files. It will then run, printing out diagnostic messages (which can be
ignored unless something goes wrong), until all the jobs have been processed.

## Running Clients

Unlike the shrink-wrapped server program, clients have to be written as Python
code. Typically, they will have the following pattern:

    from jqueue import quickstart

    if __name__ == '__main__':
        def handler(job_data):
            return get_result_from_input(job_data)

        quickstart.process_jobs(server, handler)

The `jqueue.quickstart` module provides the helper function `process_jobs`, 
which makes it simple to process jobs.

## Example

As an example, consider encoding data from WAV to MP3. There are a number of
WAV files:

    $ ls
    A.wav   B.wav   C.wav
    D.wav   E.wav   F.wav

The client uses ``ffmpeg`` to encode them:

    import os
    import tempfile

    from jqueue import quickstart

    SERVER = 'server.hostname'

    if __name__ == '__main__':
        def handler(wav_data):
            wav_in = tempfile.mktemp(suffix='.wav')
            mp3_out = tempfile.mktemp(suffix='.mp3')
            with open(wav_in, 'wb') as in_stream:
                in_stream.write(wav_data)

            os.system('ffmpeg "{}" "{}"'.format(wav_in, mp3_out))
            with open(mp3_out, 'rb') as out_stream:
                mp3_data = out_stream.read()

            os.remove(wav_in)
            os.remove(mp3_out)
            return mp3_data

        quickstart.process_jobs(SERVER, handler)

At this point, run the server:

    $ jqueue-server *.mp3

And then run as many client instances as you wish, on any machine connected
to the machine running the server. Eventually, the clients will finish their
job, and exit, leaving the following files:

    $ ls
    A.wav       A.wav.result    B.wav       B.wav.result
    C.wav       C.wav.result    D.wav       D.wav.result
    E.wav       E.wav.result    F.wav       F.wav.result

All the `*.result` files were generated by `jqueue-server`, and contain the
result of the process (namely, the MP3 encoded versions of the original WAV
files).
