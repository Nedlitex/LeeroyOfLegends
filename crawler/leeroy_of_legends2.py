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

shortCD = 10
longCD = 100
chanceForOneQuery = 12

gameQueueLimit = 1000

curl_top = 'https://prod.api.pvp.net/api/lol/na/v1.3/game/by-summoner/'
curl_bot = '/recent?api_key=a6e35da6-ae68-43ef-86d9-537de33fc2c4'

acceptedGameSubtype = ['NORMAL', 'RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_TEAM_5x5']
acceptedGameType = ['MATCHED_GAME']

# For query thread:
pendingQueries = Queue.Queue()
nextQueryEvent = threading.Event()
nextQueryEvent.clear()

# Minheap for jobs
gameQueue = []
playerGames = {}

# Dictionary for finished game
inprocessGames = {}
finishedGames = []
finishedGamesLock = threading.Lock()

# For process thread:

bigLock = threading.Lock()

threads = []

log = Queue.Queue()

def findGameQueueIndex(gameId):
  for i in range(0, len(gameQueue)):
    if (gameQueue[i][1]['gameId'] == gameId):
      return i
  return -1

def evictOldGames (toEvicts):
  offset = 0
  for i in range(0, len(gameQueue)):
    if (i in toEvicts):
      offset += 1
    else:
      gameQueue[i-offset] = gameQueue[i]
  heapq.heapify(gameQueue)

def resortGameQueue():
  for i in range(0, len(gameQueue)):
    gameId = gameQueue[i][1]
    sortIdxTime = gameQueue[i][0][0]
    sortIdxCnt = 0
    if (gameId in inprocessGames):
      sortIdxCnt = inprocessGames[gameId]['num']
    gameQueue[i] = ((sortIdxTime, sortIdxCnt), gameId)
  heapq.heapify(gameQueue)


class queryThread (threading.Thread):
  def __init__(self, tid):
    threading.Thread.__init__(self)
    self.tid = tid

  def run(self):
    while (True):
      # Wait for next query
      log.put( "[Query][TID: " + str(self.tid) + "] Wait for next query...")
      nextQueryEvent.wait()
      bigLock.acquire()
      nextQuery = pendingQueries.get()
      if (pendingQueries.empty()):
        nextQueryEvent.clear()
      else:
        nextQueryEvent.set()

      log.put("[Query][TID: " + str(self.tid) + "] Start query: " + str(nextQuery))

      bigLock.release()

      # Make the new query
      chance = chanceForOneQuery
      querySucc = False
      while (chance > 0):
        try:
          bigLock.acquire()
          timestamp = time.strftime("%Y-%m-%d-%H")
          nameFinish = timestamp + "_complete"
          nameEvict = timestamp + "_notcomplete"
          # Query
          query_url = curl_top + str(nextQuery) + curl_bot
          query = urllib2.urlopen(query_url)
          newGame = json.load(query)
          games = newGame['games']
          # Process result
          existGames = []
          for game in games:
            gameId = game['gameId']
            existGames.append(gameId)
            gameType = game['gameType']
            gameInvalid = game['invalid']
            gameLevel = game['level']
            gameSubtype = game['subType']
            log.put("[Query][TID: " + str(self.tid) + "] New data queried: " +\
                str(gameId) + ", type: " + gameType + ", subtype: " + str(gameSubtype))
            if (not gameInvalid and (gameType in acceptedGameType) and
                (gameSubtype in acceptedGameSubtype) and
                (gameLevel == 30) and
                (gameId not in finishedGames)):
              # Update inprocess games
              fellowPlayers = game['fellowPlayers']
              if (gameId not in inprocessGames):
                inprocessGames[gameId] = {'num': 0, 'stats': {}, 'fellowPlayers': fellowPlayers, 'incomplete': False}
                toInsert = ((-1 * (game['createDate'] / 1800000 * 1800000), 0), gameId)
                heapq.heappush(gameQueue, toInsert)
                log.put("[Query][TID: " + str(self.tid) + "] New game queued: " +\
                    str(gameId) + ", total queued: " + str(len(gameQueue)))
                # Update player game map
                for fellowPlayer in fellowPlayers:
                  fellow = fellowPlayer['summonerId']
                  if (fellow not in playerGames):
                    playerGames[fellow] = []
                  playerGames[fellow].append(gameId)

              if (nextQuery not in inprocessGames[gameId]['stats']):
                inprocessGames[gameId]['stats'][nextQuery] = game
                inprocessGames[gameId]['num'] += 1
                log.put("[Game][TID: " + str(self.tid) + "] Process game: " +\
                str(gameId) +\
                " added new data, now it has: " + str(inprocessGames[gameId]['num']))
              if (inprocessGames[gameId]['num'] == 10):
                finishedGames.append(gameId)
                nameOfRecord = ""
                if (inprocessGames[gameId]['incomplete']):
                  nameOfRecord = nameEvict
                else:
                  nameOfRecord = nameFinish
                with open(nameOfRecord, mode='a+') as record:
                  jsongame = json.dumps(inprocessGames[gameId]['stats']) + "\n"
                  record.write(jsongame)
                inprocessGames.pop(gameId, None)
                log.put("[Game][TID: " + str(self.tid) + "] Process game: " +\
                str(gameId) +\
                " succeeded, finished game: "+str(len(finishedGames))+", in process games: "\
                + str(len(inprocessGames)))
                # Remove game from game queue:
                gameQueueId = findGameQueueIndex(gameId)
                if (gameQueueId != -1):
                  evictOldGames([gameQueueId])
          #endfor
          querySucc = True
          toEvict0 = []
          if (nextQuery in playerGames):
            playerGame = playerGames[nextQuery]
            for playerGame0 in playerGame:
              if (playerGame0 not in existGames):
                if (playerGame0 in inprocessGames):
                  if (nextQuery not in inprocessGames[playerGame0]['stats']):
                    inprocessGames[playerGame0]['stats'][nextQuery] = {}
                    inprocessGames[playerGame0]['num'] += 1
                    inprocessGames[playerGame0]['incomplete'] = True
                    log.put("[Game][TID: " + str(self.tid) + "] Process game: " +\
                    str(playerGame0) +\
                    " added empty data, now it has: " + str(inprocessGames[playerGame0]['num']))
                  if (inprocessGames[playerGame0]['num'] == 10):
                    toEvict0.append(playerGame0)
                    finishedGames.append(playerGame0)
                    with open(nameEvict, mode='a+') as record:
                      jsongame = json.dumps(inprocessGames[playerGame0]['stats']) + "\n"
                      record.write(jsongame)
                    inprocessGames.pop(playerGame0, None)
                    log.put("[Game][TID: " + str(self.tid) + "] Process game: " +\
                    str(gameId) +\
                    " gave up, finished game: "+str(len(finishedGames))+", in process games: "\
                    + str(len(inprocessGames)))
            #end for
            if (len(toEvict0) > 0):
              evictOldGames(toEvict0)
            playerGames.pop(nextQuery, None)
          #end if
          # Evict old data
          if (len(gameQueue) >= gameQueueLimit):
            log.put("[Query][TID: " + str(self.tid) + "] Game queue is full, will evict")
            numToEvict = len(gameQueue) - gameQueueLimit
            toEvicts = heapq.nlargest(numToEvict, gameQueue)
            with open(nameEvict, mode='a+') as record:
              for toEvict in toEvicts:
                toEvictGameId = toEvict[1]
                if (toEvictGameId in inprocessGames):
                  jsongame = json.dumps(inprocessGames[toEvictGameId]['stats']) + "\n"
                  record.write(jsongame)
            evictOldGames(toEvicts)
          #end if
          resortGameQueue()
        except urllib2.HTTPError:
          log.put("[Error][TID: " + str(self.tid) +\
            "] Query failed, will sleep and try, chance: " + str(chance))
        except KeyboardInterrupt:
          print "Good bye"
          sys.exit()
        except:
          e = sys.exc_info()[0]
          log.put("[Error][TID: " + str(self.tid) +\
            "] Unexpected failure," + str(e) + " will skip")
          log.put(traceback.format_exc())
          chance = 0
        finally:
          bigLock.release()

        if (querySucc):
          log.put("[Query][TID: " + str(self.tid) + "] Query for: " + str(nextQuery)\
                + " succeeded")
          time.sleep(1)
          break
        else:
          if (chance == 0):
            log.put( "[Query][TID: " + str(self.tid) + "] Query for: " + str(nextQuery)\
                + " failed")
            time.sleep(1)
            break
          else:
            chance -= 1
            time.sleep(shortCD)

class processThread (threading.Thread):
  def __init__(self, tid):
    threading.Thread.__init__(self)
    self.tid = tid

  def run(self):
    while (True):
      # Wait for games
      log.put("[Process][TID: " + str(self.tid) + "] Wait for next game...")
      while (len(gameQueue) == 0):
        time.sleep(5)
        continue

      try:
        bigLock.acquire()
        toProcess0 = heapq.heappop(gameQueue)
        toProcessId = toProcess0[1]
        log.put("[Process][TID: " + str(self.tid) + "] Start process game: " + str(toProcessId))
        if ((toProcessId not in finishedGames) and (toProcessId in inprocessGames)):
          fellowPlayers = inprocessGames[toProcessId]['fellowPlayers']

          for player in fellowPlayers:
            playerId = player['summonerId']
            pendingQueries.put(playerId)
            nextQueryEvent.set()
            log.put("[Process][TID: " + str(self.tid) + "] Add new query: " + str(playerId))

      except KeyboardInterrupt:
          print "Good bye"
          sys.exit()
      except:
          e = sys.exc_info()[0]
          log.put("[Error][TID: " + str(self.tid) +\
            "] Unexpected failure," + str(e) + " will skip")
          log.put(traceback.format_exc())
      finally:
        bigLock.release()

class dumpThread (threading.Thread):
  def __init__(self, tid):
    threading.Thread.__init__(self)
    self.tid = tid
    self.heartbeat = time.strftime("%Y-%m-%d-%H")

  def run(self):
    while (True):
      time.sleep(60)
      try:
        bigLock.acquire()
        timestamp = time.strftime("%Y-%m-%d-%H")
        timestamp0 = time.strftime("%Y-%m-%d-%H:%M:%S")
        nameLog = timestamp + "_log"
        # dump log:
        with open(nameLog, mode='a+') as record:
          record.write("Log time: " + timestamp0 + "\n")
          while (not log.empty()):
            record.write(log.get() + "\n")
        # upload:
        if (timestamp != self.heartbeat):
          nameFinish = self.heartbeat + "_complete"
          nameEvict = self.heartbeat + "_notcomplete"
          nameLogOld = self.heartbeat + "_log"
          nameBackup = self.heartbeat + "_backup"

          self.heartbeat = timestamp
          call(["./dropbox", "upload", nameFinish, "complete/"])
          call(["./dropbox", "upload", nameEvict, "notcomplete/"])
          call(["./dropbox", "upload", nameLogOld, "log/"])
          call(["rm", nameFinish])
          call(["rm", nameEvict])
          call(["rm", nameLog])
          # Dump state
          pendingQueriesList = [pending for pending in pendingQueries.queue]
          dump = {'gameQueue': gameQueue, 'inprocessGames': inprocessGames,
                  'pendingQueries': pendingQueriesList, 'finishedGames': finishedGames,
                  'playerGames': playerGames, 'hasNextQueryEvent': nextQueryEvent.isSet()}
          jdump = json.dumps(dump)
          with open(nameBackup, mode='a+') as record:
            record.write(jdump + "\n")
          call(["./dropbox", "upload", nameBackup, "backup/"])
          call(["rm", nameBackup])

      except:
        e = sys.exc_info()[0]
        print "[Error][TID: " + str(self.tid) +\
            "] Unexpected failure"
        print traceback.format_exc()
      finally:
        bigLock.release()


#############################################
pendingQueries.put(35519913)
nextQueryEvent.set()



queryThread = queryThread(0)
threads.append(queryThread)
queryThread.daemon = True
processThread = processThread(1)
threads.append(processThread)
processThread.daemon = True
dumpThread = dumpThread(2)
threads.append(dumpThread)
dumpThread.daemon = True
queryThread.start()
processThread.start()
dumpThread.start()

while len(threads) > 0:
  try:
    threads = [t.join(1000) for t in threads if t is not None and t.isAlive()]
  except KeyboardInterrupt:
    print "Good bye"
    for t in threads:
      t.kill_received = True
    sys.exit()