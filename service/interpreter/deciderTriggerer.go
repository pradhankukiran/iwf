package interpreter

import (
	"fmt"
	"github.com/indeedeng/iwf/gen/iwfidl"
	"github.com/indeedeng/iwf/service"
	"github.com/indeedeng/iwf/service/common/compatibility"
)

func IsDeciderTriggerConditionMet(
	commandReq iwfidl.CommandRequest,
	completedTimerCmds map[int]service.InternalTimerStatus,
	completedSignalCmds map[int]*iwfidl.EncodedObject,
	completedInterStateChannelCmds map[int]*iwfidl.EncodedObject,
	completedInterStateChannelMultiCmds map[int][]*iwfidl.EncodedObject,
) bool {
	completedInterStateChannelCount := countCompletedInterStateChannelCmds(completedInterStateChannelCmds, completedInterStateChannelMultiCmds)

	if len(commandReq.GetTimerCommands())+len(commandReq.GetSignalCommands())+len(commandReq.GetInterStateChannelCommands()) > 0 {
		triggerType := compatibility.GetDeciderTriggerType(commandReq)
		if triggerType == iwfidl.ALL_COMMAND_COMPLETED {
			return len(completedTimerCmds) == len(commandReq.GetTimerCommands()) &&
				len(completedSignalCmds) == len(commandReq.GetSignalCommands()) &&
				completedInterStateChannelCount == len(commandReq.GetInterStateChannelCommands())
		} else if triggerType == iwfidl.ANY_COMMAND_COMPLETED {
			return len(completedTimerCmds)+
				len(completedSignalCmds)+
				completedInterStateChannelCount > 0
		} else if triggerType == iwfidl.ANY_COMMAND_COMBINATION_COMPLETED {
			var completedCmdIds []string
			for _, idx := range DeterministicKeys(completedTimerCmds) {
				cmdId := commandReq.GetTimerCommands()[idx].CommandId
				completedCmdIds = append(completedCmdIds, *cmdId)
			}
			for _, idx := range DeterministicKeys(completedSignalCmds) {
				cmdId := commandReq.GetSignalCommands()[idx].CommandId
				completedCmdIds = append(completedCmdIds, *cmdId)
			}
			for _, idx := range getCompletedInterStateChannelIndices(completedInterStateChannelCmds, completedInterStateChannelMultiCmds) {
				cmdId := commandReq.GetInterStateChannelCommands()[idx].CommandId
				completedCmdIds = append(completedCmdIds, *cmdId)
			}

			for _, acceptedComb := range commandReq.GetCommandCombinations() {
				acceptedCmdIds := make(map[string]int)
				for _, cid := range acceptedComb.GetCommandIds() {
					acceptedCmdIds[cid]++
				}

				for _, cid := range completedCmdIds {
					if acceptedCmdIds[cid] > 0 {
						acceptedCmdIds[cid]--
						if acceptedCmdIds[cid] == 0 {
							delete(acceptedCmdIds, cid)
						}
					}
				}
				if len(acceptedCmdIds) == 0 {
					return true
				}
			}
			return false
		} else {
			panic(fmt.Sprintf("unsupported decider trigger type: %v, this shouldn't happen as activity should have validated it", triggerType))
		}
	}
	return true
}

// countCompletedInterStateChannelCmds returns the total number of unique completed
// inter-state channel commands across both single and multi maps.
func countCompletedInterStateChannelCmds(
	single map[int]*iwfidl.EncodedObject,
	multi map[int][]*iwfidl.EncodedObject,
) int {
	seen := make(map[int]bool)
	for idx := range single {
		seen[idx] = true
	}
	for idx := range multi {
		seen[idx] = true
	}
	return len(seen)
}

// getCompletedInterStateChannelIndices returns sorted unique indices of completed
// inter-state channel commands from both single and multi maps.
func getCompletedInterStateChannelIndices(
	single map[int]*iwfidl.EncodedObject,
	multi map[int][]*iwfidl.EncodedObject,
) []int {
	merged := make(map[int]bool)
	for idx := range single {
		merged[idx] = true
	}
	for idx := range multi {
		merged[idx] = true
	}
	return DeterministicKeys(merged)
}
