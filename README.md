# One Billion Row Challenge

My solution to the [one billion row challenge](https://github.com/gunnarmorling/1brc). I wanted to see what the performance would be with mostly idiomatic use of popular crates and some light optimization.

I'm a bit disappointed with `par_split`, as it seems to only use up two of my cores. Overall time is ~18 seconds.
