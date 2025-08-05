const axios = require('axios');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const querystring = require('querystring');

const BUCKET = process.env.BUCKET_NAME;
const PLAYS_KEY = 'plays.json';
const STATS_KEY = 'stats.json';

const CLIENT_ID = process.env.SPOTIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.SPOTIFY_REFRESH_TOKEN;

async function refreshAccessToken() {
  const resp = await axios.post('https://accounts.spotify.com/api/token',
    querystring.stringify({
      grant_type: 'refresh_token',
      refresh_token: REFRESH_TOKEN
    }),
    {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    }
  );
  return resp.data.access_token;
}

async function getS3Object(key) {
  try {
    const data = await s3.getObject({ Bucket: BUCKET, Key: key }).promise();
    return JSON.parse(data.Body.toString());
  } catch (err) {
    if (err.code === 'NoSuchKey') return [];
    throw err;
  }
}

async function putS3Object(key, data) {
  await s3.putObject({
    Bucket: BUCKET,
    Key: key,
    Body: JSON.stringify(data, null, 2),
    ContentType: 'application/json'
  }).promise();
}

function aggregateStats(plays) {
    const trackCounts = {};
    const artistCounts = {};
    const dailyCounts = {};
    const dailyUniqueTracks = {}; // Tracker for daily unique tracks
    const albumPlayCounts = {};   // Tracker for album play counts
  
    plays.forEach(item => {
      const trackName = `${item.track.name} - ${item.track.artists[0].name}`;
      trackCounts[trackName] = (trackCounts[trackName] || 0) + 1;
  
      item.track.artists.forEach(artist => {
        artistCounts[artist.name] = (artistCounts[artist.name] || 0) + 1;
      });
  
      const day = item.played_at.split('T')[0];
      dailyCounts[day] = (dailyCounts[day] || 0) + 1;

      // Daily unique tracks
      const trackId = item.track?.id;
      if (trackId) {
        if (!dailyUniqueTracks[day]) dailyUniqueTracks[day] = new Set();
        dailyUniqueTracks[day].add(trackId);
      }

      // Album play count
      const album = item.track?.album;
      const albumId = album?.id;
      const artistName = album?.artists?.[0]?.name || 'Unknown Artist';
      if (albumId) {
        if (!albumPlayCounts[albumId]) {
          albumPlayCounts[albumId] = {
            name: album.name,
            artist: artistName,
            count: 0
          };
        }
        albumPlayCounts[albumId].count += 1;
      }
    });
  
    const sortedTracks = Object.entries(trackCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => ({ name, count }));
  
    const sortedArtists = Object.entries(artistCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => ({ name, count }));

    // Convert daily unique track sets to counts
    const daily_unique_tracks = {};
    for (const date in dailyUniqueTracks) {
      daily_unique_tracks[date] = dailyUniqueTracks[date].size;
    }

    // Convert album play counts to a sorted list
    const top_albums = Object.values(albumPlayCounts)
      .sort((a, b) => b.count - a.count)
  
    return {
      total_plays: plays.length,
      tracks: sortedTracks,        // ✅ all tracks, sorted by count
      artists: sortedArtists,      // ✅ all artists, sorted by count
      daily_counts: dailyCounts,
      daily_unique_tracks,         // ✅ daily unique track counts
      top_albums                   // ✅ top albums with play counts
    };
  }

exports.handler = async () => {
  try {
    const accessToken = await refreshAccessToken();

    // Fetch last 50 plays
    const resp = await axios.get('https://api.spotify.com/v1/me/player/recently-played?limit=50', {
      headers: { Authorization: 'Bearer ' + accessToken }
    });

    const newItems = resp.data.items;
    const existing = await getS3Object(PLAYS_KEY);
    const all = [...existing];

    let added = 0;
    newItems.forEach(item => {
      if (!existing.find(p => p.played_at === item.played_at)) {
        all.push(item); // store full object
        added++;
      }
    });

    if (added > 0) {
      await putS3Object(PLAYS_KEY, all);
      console.log(`✅ Saved ${added} new plays. Total: ${all.length}`);
    } else {
      console.log('ℹ️ No new plays.');
    }

    // Generate stats and save to stats.json
    const stats = aggregateStats(all);
    await putS3Object(STATS_KEY, stats);
    console.log(`📊 Stats updated. Total plays: ${stats.total_plays}`);

    return { statusCode: 200, body: `Saved ${added} new plays. Stats updated.` };

  } catch (err) {
    console.error('Error:', err.response?.data || err.message);
    return { statusCode: 500, body: 'Error fetching plays or saving stats.' };
  }
};
