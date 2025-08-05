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
  const resp = await axios.post(
    'https://accounts.spotify.com/api/token',
    querystring.stringify({
      grant_type: 'refresh_token',
      refresh_token: REFRESH_TOKEN
    }),
    {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64'),
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
  const dailyUniqueTracks = {};
  const albumPlayCounts = {};

  plays.forEach(item => {
    const trackName = `${item.track.name} - ${item.track.artists[0].name}`;
    trackCounts[trackName] = (trackCounts[trackName] || 0) + 1;

    item.track.artists.forEach(artist => {
      artistCounts[artist.name] = (artistCounts[artist.name] || 0) + 1;
    });

    const day = item.played_at.split('T')[0];
    dailyCounts[day] = (dailyCounts[day] || 0) + 1;

    const trackId = item.track?.id;
    if (trackId) {
      if (!dailyUniqueTracks[day]) dailyUniqueTracks[day] = new Set();
      dailyUniqueTracks[day].add(trackId);
    }

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

  const daily_unique_tracks = {};
  for (const date in dailyUniqueTracks) {
    daily_unique_tracks[date] = dailyUniqueTracks[date].size;
  }

  const top_albums = Object.values(albumPlayCounts).sort((a, b) => b.count - a.count);

  return {
    total_plays: plays.length,
    tracks: sortedTracks,
    artists: sortedArtists,
    daily_counts: dailyCounts,
    daily_unique_tracks,
    top_albums
  };
}

async function getSpotifyTop(type, term, accessToken) {
  const url = `https://api.spotify.com/v1/me/top/${type}?limit=50&time_range=${term}`;
  const res = await axios.get(url, {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  return res.data.items.map(item => ({
    name: item.name,
    ...(type === 'artists' ? {} : { artist: item.artists[0].name })
  }));
}

async function getSpotifyStats(accessToken) {
  const terms = ['short_term', 'medium_term', 'long_term'];

  const requests = terms.flatMap(term => [
    getSpotifyTop('tracks', term, accessToken),
    getSpotifyTop('artists', term, accessToken)
  ]);

  const results = await Promise.allSettled(requests);

  const spotifyStats = {};
  for (let i = 0; i < terms.length; i++) {
    const [tracksResult, artistsResult] = [results[i * 2], results[i * 2 + 1]];
    spotifyStats[terms[i]] = {
      top_tracks: tracksResult.status === 'fulfilled' ? tracksResult.value : [],
      top_artists: artistsResult.status === 'fulfilled' ? artistsResult.value : []
    };
  }

  return spotifyStats;
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
        all.push(item);
        added++;
      }
    });

    if (added > 0) {
      await putS3Object(PLAYS_KEY, all);
      console.log(`✅ Saved ${added} new plays. Total: ${all.length}`);
    } else {
      console.log('ℹ️ No new plays.');
    }

    const myStats = aggregateStats(all);
    const spotifyStats = await getSpotifyStats(accessToken);

    const finalStats = {
      my_stats: {
        short_term: myStats,
        medium_term: myStats,
        long_term: myStats
      },
      spotify_stats: spotifyStats
    };

    await putS3Object(STATS_KEY, finalStats);
    console.log('📊 Stats saved to S3.');

    return {
      statusCode: 200,
      body: `Saved ${added} new plays. Stats updated.`
    };
  } catch (err) {
    console.error('Spotify error, skipping update this run:', err.response?.data || err.message);
    return {
      statusCode: 500,
      body: 'Error fetching plays or Spotify stats.'
    };
  }
};
