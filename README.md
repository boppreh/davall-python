# DAV-all

Create a WebDAV server out of SQLite databases, JSON files, .zip, CSV, etc.

DISCLAIMER: this was an experiment to play around with Claude Code, which was used *exclusively*. I didn't write or edit any code myself, and I've only reviewed the code *very* superficially. Not fully "vibe coded", but certainly insufficient to claim it as my own. Each commit from Claude contains the full text of my prompt that lead to that commit.

Developed with Claude Code 2.1.42, model Opus 4.6, no custom Claude.MD or skills. It hit session usage limits shortly afer https://github.com/boppreh/davall-python/commit/0a42c6ae613db00a17620420b14ac09cf26b7341 , and had to be interrupted twice when going on directions I didn't like. Here was the initial prompt, that resulted in an usable MVP in 10 minutes 19 seconds, completely independently:

> This is a currently empty project. The goal is to create a read-only WebDAV interface to a number of different non-filesystem backends, such as JSON, SQLite, archives (zip, tar.gz, etc), and more. The idea is that someone could "mount" a SQLite database using this project and explore the tables and the rows using their file manager, for example. This initial version will use Python without any external dependencies, only the standard library. The code should be as simple and as robust as possible. Errors in the underlying backend should be considered, and propagated correctly to the WebDAV "frontend". Remember that only read operations should be supported.
> 
> 0. Read the WebDAV spec to understand what is possible and allowed.
> 1. Start by considering what "backends" would be useful to support, and which ones are supported by the Python standard library. Don't be afraid of making out-of-the-box suggestions, like "mounting" an image to exposing EXIF data.
> 2. From these backends, consider what WebDAV operations would be useful to at least a majority of them, and which ones wouldn't make sense.
> 3. Write this plan down in a .md file and commit it.
> 4. Create the WebDAV frontend initially using only a simple memory-backed JSON-like structure. Write tests for it, and commit when done.
> 5. For each suggested backend, in order of usefulness, implement it as simply and robustly as possible. If an aspect is unclear, too complicated, or otherwise not a good idea for this MVP, raise a TODO-like error. Keep it down to the basics for now. Write tests that create the various backend objects and test them. Commit after each implementation.