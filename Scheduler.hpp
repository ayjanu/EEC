//
//  Scheduler.hpp
//  CloudSim
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <set>
#include <deque>
#include "Interfaces.h"
#include "Internal_Interfaces.h"

class Scheduler {
public:
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void StateChangeComplete(Time_t time, MachineId_t machine_id);

    // Data structures (made public for SLAWarning function)
    std::map<TaskId_t, VMId_t> task_vm_map;            // VM for each task
    std::map<VMId_t, MachineId_t> vm_machine_map;      // Machine for each VM
    std::set<MachineId_t> pending_state_changes;       // Machines with pending state changes
    std::set<VMId_t> pending_migrations;               // VMs currently being migrated

private:
    // Constants
    static const Time_t SLA_THRESHOLD;
    static const double LOAD_THRESHOLD_LOW;
    static const double LOAD_THRESHOLD_HIGH;
    static const unsigned INITIAL_ACTIVE_MACHINES;
    static const MachineId_t INVALID_MACHINE = static_cast<MachineId_t>(-1);
    
    // Helper methods
    bool AreAllMachinesBusy();
    void ProcessPendingTasks(Time_t now);
    MachineId_t FindBestMachine(const TaskInfo_t& task_info);
    MachineId_t PowerOnNewMachine();
    VMId_t FindOrCreateVM(MachineId_t machine_id, CPUType_t cpu_type);
    void AdjustMachinePerformance(MachineId_t machine_id, double urgency);
    void UpdateMachinePerformance(MachineId_t machine_id, Time_t now);
    bool CheckSLAViolations(MachineId_t machine_id, Time_t now);
    void CheckMachinePowerState(MachineId_t machine_id);
    void CheckClusterLoad();
    
    // Data structures
    std::vector<unsigned> machine_task_count;          // Number of tasks per machine
    std::vector<std::vector<VMId_t>> machine_vm_map;   // VMs on each machine
    std::deque<TaskId_t> pending_tasks;                // Tasks waiting for resources
};

// Public interface functions called by the simulator
extern void InitScheduler();
extern void HandleNewTask(Time_t time, TaskId_t task_id);
extern void HandleTaskCompletion(Time_t time, TaskId_t task_id);
extern void MemoryWarning(Time_t time, MachineId_t machine_id);
extern void MigrationDone(Time_t time, VMId_t vm_id);
extern void SchedulerCheck(Time_t time);
extern void SimulationComplete(Time_t time);
extern void SLAWarning(Time_t time, TaskId_t task_id);
extern void StateChangeComplete(Time_t time, MachineId_t machine_id);

#endif /* Scheduler_hpp */