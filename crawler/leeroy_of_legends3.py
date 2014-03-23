import Queue
import threading
import time
from datetime import datetime
import urllib2
import json
import heapq
import sys
import traceback
from subprocess import call
import copy
import os

curl_top = 'https://prod.api.pvp.net/api/lol/na/v1.3/game/by-summoner/'
curl_bot = '/recent?api_key=a6e35da6-ae68-43ef-86d9-537de33fc2c4'

acceptedGameSubtype = ['NORMAL', 'RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_TEAM_5x5']
acceptedGameType = ['MATCHED_GAME']

gameQueueLimit = 300

def make_game(game):
  toInsert = {'key'   : (10, -1 * (game['createDate'] / 1800000 * 1800000)),
              'id'    : game['gameId'],
              'data'  : {},
              'fellow' : game['fellowPlayers'],
              'incomplete' : False}
  return toInsert

def insert_game (l, v):
  k = v['key']
  for i in xrange(len(l)):
    vi = l[i]
    ki = vi['key']
    if (k <= ki):
      l.insert(i, v)
      return
  l.append(v)

def delete_index (l, i):
  del l[i]

def delete_gId (l, gId):
  for i in xrange(len(l)):
    if (l[i]['id'] == gId):
      del l[i]
      return

def find_game (l, gId):
  for i in xrange(len(l)):
    if (l[i]['id'] == gId):
      return i
  return -1

def is_game_exist (l, gId):
  if (find_game(l, gId) == -1):
    return False
  return True

def make_timestamp ():
  timestamp = time.strftime("%Y-%m-%d-%H")
  now = datetime.now()
  now_minute = (now.minute / 5 * 5)
  timestamp += ('-' + str(now_minute))
  return timestamp

gameQueue = []
summonerQueue = []
log = []
playerGameMap = {}

max_query = 12

old_timestamp = make_timestamp()

max_fingame_perfile = 10000000
finished_game_count = 0

complete_game_count = 0
incomplete_game_count = 0
evict_game_count = 0

queueHeadEvicted = False

def mark_game_finished (gId):
  global finished_game_count
  hashIdx = gId / max_fingame_perfile
  hashFile = "finishedGames_" + str(hashIdx)
  with open(hashFile, mode='a+') as f:
    f.write(str(gId) + "\n")
  finished_game_count += 1

def is_game_finished (gId):
  hashIdx = gId / max_fingame_perfile
  hashFile = "finishedGames_" + str(hashIdx)
  if (not os.path.exists(hashFile)):
    return False
  else:
    with open(hashFile, mode='r') as f:
      ffs = f.readlines()
      for ff in ffs:
        if (int(ff) == gId):
          return True
  return False

def insert_fellow_game (timestamp, game, summoner, incomplete=False):
  global complete_game_count
  global incomplete_game_count
  global queueHeadEvicted

  gameId = game['gameId']
  index = find_game (gameQueue, gameId)
  if (summoner not in gameQueue[index]['data']):
    gameQueue[index]['data'][summoner] = game
  if (not gameQueue[index]['incomplete']):
    gameQueue[index]['incomplete'] = incomplete
  log.append("[" + timestamp +"][IO]Game: " +\
        str(gameId) + " has " + str(len(gameQueue[index]['data'])) + " data")
  if (len(gameQueue[index]['data']) == 10):
    mark_game_finished(gameId)
    nameOfRecord = timestamp + "_complete"
    if (gameQueue[index]['incomplete']):
      incomplete_game_count += 1
      nameOfRecord = timestamp + "_incomplete"
    else:
      complete_game_count += 1
    log.append("[" + timestamp +"][IO]Game: " +\
        str(gameId) + " finished, will be write to: " + nameOfRecord +\
        ", before write, game queue len: " + str(len(gameQueue)) +\
        ", head of queue: " + str(gameQueue[0]['id']))
    with open(nameOfRecord, mode='a+') as record:
      toWrite = {'gameId': gameQueue[index]['id'], 'gameData': gameQueue[index]['data']}
      jsongame = json.dumps(toWrite) + "\n"
      record.write(jsongame)
    delete_index(gameQueue, index)
    if (index == 0):
      queueHeadEvicted = True
    log.append("[" + timestamp +"][IO]Game: " +\
        str(gameId) + " after write, game queue len: " + str(len(gameQueue))+\
        ", head of queue: " + str(gameQueue[0]['id']))
  elif (len(gameQueue[index]['data']) > 10):
    log.append("[Error]: Game " + str(gameId) + " has more than 10 data")
  else:
    game = gameQueue[index]
    delete_index(gameQueue, index)
    game['key'][0] -= 1
    insert_game(gameQueue, game)

def clear_playergame ():
  toClear = []
  for summoner in playerGameMap:
    games = playerGameMap[summoner]
    if (len(games) == 0):
      toClear.append(summoner)
    else:
      cnt = 0
      newGame = []
      for game in games:
        if (is_game_exist(gameQueue, game)):
          cnt += 1
          newGame.append(game)
      if (cnt == 0):
        toClear.append(summoner)
      else:
        playerGameMap[summoner] = newGame
  for c in toClear:
    del playerGameMap[c]

def evict_game_queue (timestamp):
  global evict_game_count
  toEvictLen = len(gameQueue) - gameQueueLimit
  if (toEvictLen <= 0):
    return
  nameOfRecord = timestamp + "_incomplete"
  with open(nameOfRecord, mode='a+') as record:
    for i in range(0, toEvictLen):
      toEvictGame = gameQueue[len(gameQueue) - 1]
      mark_game_finished(toEvictGame['id'])
      evict_game_count += 1
      log.append("[" + timestamp +"][IO]Game: " +\
        str(toEvictGame['id']) + " evicted, will be write to: " + nameOfRecord +\
        ", before write, game queue len: " + str(len(gameQueue)) +\
        ", head of queue: " + str(gameQueue[0]['id']))
      toWrite = {'gameId': toEvictGame['id'], 'gameData': toEvictGame['data']}
      jsongame = json.dumps(toWrite) + "\n"
      record.write(jsongame)
      del gameQueue[len(gameQueue) - 1]
      log.append("[" + timestamp +"][IO]Game: " +\
        str(toEvictGame['id']) + " after write, game queue len: " + str(len(gameQueue))+\
        ", head of queue: " + str(gameQueue[0]['id']))
  clear_playergame()

################################################################################
Alive = True
summonerQueue.append(21692722)
oldSec = time.strftime("%Y-%m-%d-%H:%M:") + str(datetime.now().second / 30 * 30)
oldQueueHead = -1
while (Alive):

  timestamp = make_timestamp()
  log.append("["+timestamp+"] Start processing pending summoners...")
  queueHeadEvicted = False
  while (len(summonerQueue) != 0):
    summoner = summonerQueue[0]
    del summonerQueue[0]

    chance = max_query

    while (chance > 0):
      try:
        timestamp = make_timestamp()
        log.append("["+timestamp+"] Attemp to query for summoner: " + str(summoner) +\
          ", chance left: " + str(chance))
        query_url = curl_top + str(summoner) + curl_bot
        query = urllib2.urlopen(query_url)
        query_parse = json.load(query)
        games = query_parse['games']
        log.append("[" + timestamp +"] Query for summoner: " + str(summoner) + " succeeded")

        existGames = []
        for game in games:
          gameId = game['gameId']
          existGames.append(gameId)
          gameType = game['gameType']
          gameInvalid = game['invalid']
          gameLevel = game['level']
          gameSubtype = game['subType']
          log.append("[" + timestamp +"] \tQueried game: " +\
              str(gameId) + ", type: " + gameType + ", subtype: " + str(gameSubtype))
          if (not gameInvalid and (gameType in acceptedGameType) and
              (gameSubtype in acceptedGameSubtype) and
              (gameLevel == 30) and
              (not is_game_finished(gameId))):
            # Update inprocess games
            fellowPlayers = game['fellowPlayers']
            if (not is_game_exist(gameQueue, gameId)):
              gameToInsert = make_game(game)
              insert_game(gameQueue, gameToInsert)
              log.append("[" + timestamp +"] \t Game: " +\
              str(gameId) + ", inserted into game queue")
              # Update playe game map
              for fp in fellowPlayers:
                f = fp['summonerId']
                if (f not in playerGameMap):
                  playerGameMap[f] = []
                if (gameId not in playerGameMap[f]):
                  playerGameMap[f].append(gameId)
              #endfor
            #endif
            insert_fellow_game(timestamp, game, summoner)
          #endif
          else:
            log.append("[" + timestamp +"] \tGame: " +\
              str(gameId) + " is invalid, will not continue")
        #endfor
        # Evict impossible games
        toEvict = []
        if (summoner in playerGameMap):
          for playerGame in playerGameMap[summoner]:
            if (is_game_exist(gameQueue, playerGame) and (playerGame not in existGames)):
              insert_fellow_game(timestamp, {'gameId': playerGame}, summoner, True)
          #endfor
          del playerGameMap[summoner]
        #endif

        # Evict out-of-bound games
        if (len(gameQueue) >= gameQueueLimit):
          evict_game_queue(timestamp)
        #endif
        time.sleep(1)
        break
      except urllib2.HTTPError:
          log.put("[Error] Query failed, will sleep and try, chance: " + str(chance))
          time.sleep(10)
      except:
        chance = 0
        e = sys.exc_info()[0]
        log.append("[Error] Failure," + str(e) +" will give up")
        log.append(traceback.format_exc())
        time.sleep(1)
    #endwhile
    log.append("["+timestamp+"] Attemp to query for summoner: " + str(summoner) +\
          " finished")
  #endwhile
  log.append("["+timestamp+"] Finished all pending summoners")
  # Fill summoner queue
  if (len(summonerQueue) == 0):
    log.append("["+timestamp+"] Start processing pending games...")
    if (len(gameQueue) == 0):
      log.append("["+timestamp+"] No more pending games, will exit...")
      Alive = False
    else:
      toProcess = gameQueue[0]
      if (oldQueueHead != -1):
        if (oldQueueHead == toProcess['id']):
          nameOfRecord = timestamp + "_incomplete"
          with open(nameOfRecord, mode='a+') as record:
            toWrite = {'gameId': gameQueue[0]['id'], 'gameData': gameQueue[0]['data']}
            jsongame = json.dumps(toWrite) + "\n"
            record.write(jsongame)
          incomplete_game_count += 1
          del gameQueue[0]
          toProcess = gameQueue[0]
      oldQueueHead = toProcess['id']
      log.append("["+timestamp+"] \tPick game: " + str(toProcess['id']))
      if (is_game_finished(toProcess['id'])):
        log.append("["+timestamp+"] \tGame: " + str(toProcess['id']) + " has already finished")
      else:
        fellowPlayers = toProcess['fellow']
        for fp in fellowPlayers:
          fpId = fp['summonerId']
          summonerQueue.append(fpId)
  #endif
  # Dump info:
  newSec = time.strftime("%Y-%m-%d-%H:%M:") + str(datetime.now().second / 30 * 30)
  if ((not Alive) or (newSec != oldSec)):
    oldSec = newSec
    nameLog = timestamp + "_log"
    nameStat = timestamp + "_stat"
    with open(nameLog, mode='a+') as record:
      record.write("Log time: " + time.strftime("%Y-%m-%d-%H:%M:%S") + "\n")
      for lg in log:
        record.write(lg + "\n")
      log[:] = []
    with open(nameStat, mode='a+') as record:
      record.write("--- System status report @ " + time.strftime("%Y-%m-%d-%H:%M:%S") + "---\n")
      record.write("\t- Finished game count: " + str(finished_game_count) + "\n")
      record.write("\t\t- Completed: " + str(complete_game_count) + "\n")
      record.write("\t\t- Incomplete: " + str(incomplete_game_count) + "\n")
      record.write("\t\t- Evicted: " + str(evict_game_count) + "\n")
      record.write("\t- Game queue count: " + str(len(gameQueue)) + "\n")
      record.write("\t- Pending summoner count: " + str(len(summonerQueue)) + "\n")
      record.write("\t- Player-game map size: " + str(len(playerGameMap)) + "\n")
  #endif
  #Upload:
  if ((not Alive) or timestamp != old_timestamp):
    nameFinish = old_timestamp + "_complete"
    nameEvict = old_timestamp + "_incomplete"
    nameLogOld = old_timestamp + "_log"
    nameStatOld = old_timestamp + "_stat"
    nameBackup = old_timestamp + "_backup"

    old_timestamp = timestamp
    try:
      call(["./dropbox", "upload", nameFinish, "complete/"])
      try:
        call(["rm", nameFinish])
      except:
        print "Failed to remove complete"
    except:
      print "Failed to upload complete"
    try:
      call(["./dropbox", "upload", nameEvict, "notcomplete/"])
      try:
        call(["rm", nameEvict])
      except:
        print "Failed to remove not complete"
    except:
      print "Failed to upload notcomplete"
    try:
      call(["./dropbox", "upload", nameLogOld, "log/"])
      try:
        call(["rm", nameLogOld])
      except:
        print "Failed to remove log"
    except:
      print "Failed to upload log"
    try:
      call(["./dropbox", "upload", nameStatOld, "log/"])
      try:
        call(["rm", nameStatOld])
      except:
        print "Failed to remove stat"
    except:
      print "Failed to upload stat"
    # Dump state
    dump = {'gameQueue': gameQueue,
            'summonerQueue' : summonerQueue,
            'finished_game_count' : finished_game_count,
            'complete_game_count' : complete_game_count,
            'incomplete_game_count' : incomplete_game_count,
            'evict_game_count' : evict_game_count,
            'playerGameMap': playerGameMap}
    jdump = json.dumps(dump)
    with open(nameBackup, mode='a+') as record:
      record.write(jdump + "\n")
    try:
      call(["./dropbox", "upload", nameBackup, "backup/"])
      try:
        call(["rm", nameBackup])
      except:
        print "Failed to remove backup"
    except:
      print "Failed to upload backup"

