//
//  Scheduler.hpp
//  CloudSim
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <string>

#include "Interfaces.h"
#include "SimTypes.h"

class Scheduler {
public:
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    
    // Helper functions
    VMId_t FindBestVM(TaskId_t task_id, TaskClass_t taskClass, CPUType_t requiredCPU, 
                     VMType_t requiredVM, bool needsGPU, unsigned memoryNeeded);
    VMId_t CreateNewVM(VMType_t vmType, CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded);
    void ConsolidateVMs(Time_t now);
    void UpdateMachineUtilization();
    double CalculateMachineUtilization(MachineId_t machineId);
    void MigrateVMFromOverloadedMachine(MachineId_t machineId);
    void AdjustMachinePowerState(MachineId_t machineId, double utilization);
    MachineId_t FindSuitableMachine(CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded);
    MachineId_t PowerOnMachine(CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded);
    MachineId_t FindMigrationTarget(VMId_t vm, MachineId_t sourceMachine);
    bool IsVMMigrating(VMId_t vm) const;
    bool HasActiveJobs(MachineId_t machineId);
    bool EnsureMachineAwake(MachineId_t machineId);
    bool IsCompatibleVMCPU(VMType_t vmType, CPUType_t cpuType);
    void CreateVMsForAllCPUTypes();
    std::vector<VMId_t> GetCompatibleVMs(CPUType_t cpuType, VMType_t vmType);
    bool PrepareVMForMigration(VMId_t vm);
    void HandleSLAWarning(Time_t time, TaskId_t task_id);
    void HandleStateChangeComplete(Time_t time, MachineId_t machine_id);
    bool RequestMachineStateChange(MachineId_t machineId, MachineState_t newState);
    
    // Task information helper functions
    TaskClass_t GetTaskClass(TaskId_t taskId);
    bool IsGPUCapable(TaskId_t taskId);
    unsigned GetMemory(TaskId_t taskId);
    CPUType_t RequiredCPUType(TaskId_t taskId);
    VMType_t RequiredVMType(TaskId_t taskId);
    SLAType_t RequiredSLA(TaskId_t taskId);
    
private:
    // Configuration thresholds
    const double HIGH_UTIL_THRESHOLD = 0.8;
    const double LOW_UTIL_THRESHOLD = 0.3;
    const Time_t CONSOLIDATION_INTERVAL = 300000;     // Check for consolidation every 5 minutes
    Time_t lastConsolidationTime = 0;
    
    // Data structures
    std::vector<VMId_t> vms;
    std::vector<MachineId_t> machines;
    std::map<TaskClass_t, std::vector<VMId_t>> taskClassToVMs;
    std::map<MachineId_t, double> machineUtilization;
    std::map<CPUType_t, std::vector<MachineId_t>> cpuTypeMachines;
    std::map<VMId_t, MachineId_t> vmToMachine;
    std::set<MachineId_t> activeMachines;
    std::set<VMId_t> migratingVMs;
    std::map<TaskId_t, bool> slaViolatedTasks;  // Track tasks with SLA violations
    std::map<MachineId_t, bool> pendingStateChanges;  // Track machines with pending state changes
    std::set<TaskId_t> highPriorityTasks;
};

#endif /* Scheduler_hpp */