#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <queue>
#include <algorithm>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void MemoryWarningHandler(Time_t time, MachineId_t machine_id);

    // Made public so SLAWarning can access it
    map<TaskId_t, VMId_t> task_to_vm;

private:
    vector<VMId_t> vms;
    vector<MachineId_t> machines;
    
    // Track VM load and task assignments
    map<VMId_t, unsigned> vm_load;
    map<VMId_t, vector<TaskId_t>> vm_tasks;
    
    // Track machine states and capabilities
    map<MachineId_t, bool> machine_has_gpu;
    map<MachineId_t, MachineState_t> machine_states;
    map<MachineId_t, CPUType_t> machine_cpus;
    // SLA tracking
    map<TaskId_t, SLAType_t> task_sla;
    map<TaskId_t, Time_t> task_arrival;
    
    // Energy tracking
    Time_t last_energy_check;
    double last_cluster_energy;
    
    // Helper methods
    VMId_t findBestVMForTask(TaskId_t task_id);
    void adjustMachineStates();
    void migrateVMIfNeeded(Time_t now);
};

#endif /* Scheduler_hpp */