package main

import (
	"fmt"

	storage "github.com/kc8/dump-1090-aggergator/storage"
	queue "github.com/kc8/kc_go_queue"
)

type TaskType int

const (
	ADD           = 1
	DELETE        = 2
	SEARCH        = 3
	CLEAN         = 4
	UPDATE_OR_ADD = 5
)

type Task struct {
	item     storage.MapItem[CollectedData]
	raw      *FormattedAdbsMsg
	key      string
	taskType TaskType
	visitFn  storage.DoPerEntry[CollectedData]
}

type modifyStoQueue struct {
	queue *queue.Queue[Task]
	backendSto *storage.MapStorage[CollectedData]
}

func NewQueue(sto *storage.MapStorage[CollectedData]) *modifyStoQueue {
	return &modifyStoQueue{
		queue:      queue.New[Task](),
		backendSto: sto,
	}
}

func (q *modifyStoQueue) append(item storage.MapItem[CollectedData]) {
	task := Task{
		taskType: ADD,
		item:     item,
	}
	q.queue.Enqueue(task)
}

func (q *modifyStoQueue) delete(item storage.MapItem[CollectedData]) {
	task := Task{
		taskType: DELETE,
		item:     item,
	}
	q.queue.Enqueue(task)
}

func (q *modifyStoQueue) addSearch(key string) {
	task := Task{
		taskType: SEARCH,
		key:      key,
	}
	q.queue.Enqueue(task)
}

func (q *modifyStoQueue) checkForReadyToDelete(visitFn storage.DoPerEntry[CollectedData]) {
	task := Task{
		taskType: CLEAN,
		key:      "",
		visitFn:  visitFn,
	}
	q.queue.Enqueue(task)
}

func (q *modifyStoQueue) updateOrAdd(raw *FormattedAdbsMsg) {
	task := Task{
		taskType: UPDATE_OR_ADD,
		key:      raw.AircraftICAOAddr,
		raw:      raw,
	}
	q.queue.Enqueue(task)
}

func (q *modifyStoQueue) run(fndChan chan Nullable[storage.MapItem[CollectedData]]) {
	arr := make([]string, 0)
	for {
		currentTask := q.queue.Deque()
		if currentTask.taskType == 0 {
			continue
		}
		if currentTask.taskType == ADD {
			for _, k := range arr {
				if k == currentTask.item.Data.Icao {
					Log(currentTask.item.Data.Icao, ERROR)
					panic("DUPE ICAO")
				}
			}
			Log(currentTask.item.Data.Icao, ERROR)
			arr = append(arr, currentTask.item.Data.Icao)
			if err := sto.Insert(currentTask.item, simpleKeyCompare); err != nil {
				Log(err.Error(), ERROR)
			}
		}
		if currentTask.taskType == UPDATE_OR_ADD {
			foundItem, findErr := q.backendSto.Search(currentTask.key, simpleKeyCompare)
			if findErr != nil { // okay to add
				sto.Insert(createNewDataEntry(currentTask.raw), simpleKeyCompare)
			} else { // update
				sto.Insert(updateEntry(foundItem, currentTask.raw), simpleKeyCompare)
			}
		}
		if currentTask.taskType == DELETE {
			nodeKey := currentTask.item.Key
			if _, delErr := sto.Delete(currentTask.item.Key, simpleKeyCompare); delErr != nil {
				Log(fmt.Sprintf("Could not remove entry from storage due to one of the following."+
					"(1) Errmsg: %s (2): item key %s", delErr.Error(), nodeKey), ERROR)
			} else {
				Log(fmt.Sprintf("Removed Entry from storage %s", nodeKey), INFO)

			}
		}
		if currentTask.taskType == CLEAN {
			sto.Traverse(currentTask.visitFn)
		}
		if currentTask.taskType == SEARCH {
			foundItem, findErr := q.backendSto.Search(currentTask.key, simpleKeyCompare)
			if findErr != nil {
				fndChan <- Nullable[storage.MapItem[CollectedData]]{
					Valid:    false,
					maybeErr: findErr,
				}
			} else {
				fndChan <- Nullable[storage.MapItem[CollectedData]]{
					Value:    foundItem,
					Valid:    true,
					maybeErr: findErr,
				}
			}
		}
	}
}
