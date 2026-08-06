# South Asia IPTV

An automatically built and verified IPTV playlist, focused on Pakistani and Indian
channels, with the major Indian networks, kids/cartoon channels, movie channels and
documentary channels alongside.

Rebuilt **twice a day** (02:00 and 14:00 UTC = 07:00/19:00 PKT, 07:30/19:30 IST) by
GitHub Actions, which re-probes every stream and drops the ones that have died.

## Playlists

| File | Contents |
| --- | --- |
| `playlist.m3u` | Everything |
| `playlists/SouthAsia.m3u` | Pakistani, Indian, Bangladeshi, Sri Lankan, Nepali + the Indian networks |
| `playlists/Pakistan.m3u` | Pakistani channels |
| `playlists/India.m3u` | Indian channels + networks |
| `playlists/Networks.m3u` | Star, Sony, Colors, Zee |
| `playlists/Movies.m3u` | Harry Potter & fantasy, blockbusters, UK & British, Bollywood |
| `playlists/Series.m3u` | HBO & prestige drama, single-show binge channels |
| `playlists/Kids.m3u` | Cartoons, kids, retro/classic cartoons |
| `playlists/Sports.m3u` | Cricket, South Asian and international sport |
| `playlists/Documentary.m3u` | Discovery, National Geographic, history, nature |
| `playlists/News.m3u` | News channels |

Point TiviMate (or any M3U player) at the raw URL of whichever file you want.

## Channel groups

Grouping is data-driven — see `channel_groups` in `config.json`. Rules are ordered
and the first match wins, so `Star Sports` lands in the cricket group rather than in
`Star Network`. Anything that matches no rule falls back to `<Country> <Category>`.

Dedicated groups include: Sports (Cricket & South Asia), Sports (International),
HBO & Prestige Drama, Binge TV (Shows & Box Sets), Harry Potter & Fantasy Movies,
Blockbuster Movies, UK & British Movies, Star Network, Sony Network, Colors Network,
Zee Network, Bollywood & Indian Movies, Cartoons & Kids, Retro & Classic Cartoons,
and Discovery & Documentary.

To add a channel you care about, add a regex to the relevant rule's `match` list —
nothing in `build.py` needs to change.

### A note on Cartoon Network India, Pogo and Boomerang

These are paid JioStar / Warner Bros. Discovery channels and no public free playlist
carries them; the rules that would pick them up are already in place, so they will
appear automatically if a source ever starts offering them. In the meantime
`Cartoons & Kids` and `Retro & Classic Cartoons` carry Nickelodeon, Nick Jr,
Nicktoons, Disney Channel/XD/Junior, Cartoonito, Toonami Aftermath,
Pluto TV Retro Toons, Cartoon Classics, Tom & Jerry, Popeye, The Smurfs,
Garfield, Mr Bean and similar.

### A note on Game of Thrones and HBO

HBO keeps its flagship dramas — Game of Thrones, House of the Dragon,
A Knight of the Seven Kingdoms — exclusive to HBO Max, and there is no HBO linear
feed on any public source either (the only `HBO` match in the entire iptv-org index
is an archive boxing channel). Those title patterns stay in the `HBO & Prestige Drama`
rule so a real channel would be picked up automatically.

What that group does deliver is the premium cable drama networks that *are* public:
AMC, AMC+, Stories by AMC, Showtime, Starz, FX/FXX, SYFY, IFC, Sundance TV,
Paramount Network, BBC America and Viasat Epic Drama. `Binge TV (Shows & Box Sets)`
carries the single-show 24/7 channels — Star Trek (all series), Stargate,
The Walking Dead Universe, Doctor Who Classic, CSI, NCIS, Baywatch, Survivor,
Xena, Hercules, MacGyver, Frasier, Cheers and more.

There is likewise no licensed 24/7 Harry Potter channel on public sources.
`Harry Potter & Fantasy Movies` collects the fantasy/sci-fi movie channels that show
that kind of programming, and the `harry potter` / `wizarding` patterns stay in the
rule so a real one would be picked up automatically.

## Sources

The primary source is the [iptv-org API](https://github.com/iptv-org/api)
(`channels.json` + `streams.json` + `logos.json`), which is a superset of every
iptv-org `.m3u` and carries country, category, quality, user-agent and referrer
metadata. Several community playlists are merged on top — see `sources.m3u` in
`config.json`. A source that fails to fetch is skipped; the run only aborts if
every source fails.

## How the build stays honest

* Each source fetch is retried (`source_retries`), and a failing source is skipped
  rather than aborting the build.
* Every stream gets `stream_attempts` probes; HTML portal pages, empty manifests
  and DRM-protected entries are rejected.
* `reports/health.json` tracks consecutive failures. A stream that worked recently
  survives `grace_failures` bad runs before it is dropped, so a transient upstream
  blip does not gut the playlist.
* The CI runner is in the US, and many Pakistani/Indian servers refuse connections
  from outside their region. Those are recorded in `reports/region_locked.txt` and
  kept rather than called dead — they play at home. A stream that has *never* been
  reachable is dropped after `unverifiable_max_fails` runs so rot cannot accumulate.
  Set `keep_geo_blocked` to `false` to ship only what CI could actually reach.
* A safety gate refuses to overwrite `playlist.m3u` if the channel count collapses
  below `safety.min_channels` or drops by more than `safety.max_drop_ratio`.
* Output files are written atomically.

## Reports

`reports/stats.json` (counts per group/category/country plus per-source status),
`reports/dead_channels.txt`, `reports/region_locked.txt`, `reports/sources.json`,
`reports/health.json`. Each Actions run also posts a summary table to the job page.

## Running locally

```bash
pip install -r requirements.txt

python build.py               # full build with verification (~3 minutes)
python build.py --no-verify   # fast dry run, skips probing
python build.py --limit 200   # only process the first 200 candidates
python summarize.py           # print the Markdown summary of the last build
```
