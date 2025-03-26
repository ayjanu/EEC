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

#include "Interfaces.h"
#include "SimTypes.h"

class Scheduler {
public:
    const std::vector<VMId_t>& GetVMs() const { return vms; }
    const std::vector<MachineId_t>& GetMachines() const { return machines; }
    bool IsMachineActive(MachineId_t machine) const { 
        return activeMachines.find(machine) != activeMachines.end(); 
    }
    bool SafeRemoveTask(VMId_t vm, TaskId_t task);
    void ActivateMachine(MachineId_t machine) {
        activeMachines.insert(machine);
        machineUtilization[machine] = 0.0;
    }
    void DeactivateMachine(MachineId_t machine) {
        activeMachines.erase(machine);
    }
    
    void AddVM(VMId_t vm) {
        vms.push_back(vm);
    }
    void MarkVMAsMigrating(VMId_t vm) {
        migratingVMs.insert(vm);
    }
    
    void MarkVMAsReady(VMId_t vm) {
        migratingVMs.erase(vm);
    }
    
    bool IsVMMigrating(VMId_t vm) const {
        return migratingVMs.find(vm) != migratingVMs.end();
    }
    bool IsMigrationInProgress() const { return !migratingVMs.empty(); }
    void ConsolidateVMs(Time_t now);
    
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t time);
    void TaskComplete(Time_t now, TaskId_t task_id);
    
private:
    // Thresholds for underload/overload detection
    const double UNDERLOAD_THRESHOLD = 0.3;  // 30% utilization
    const double OVERLOAD_THRESHOLD = 0.8;   // 80% utilization
    
    // Track machine utilization
    std::map<MachineId_t, double> machineUtilization;
    
    // Track which machines are powered on
    std::set<MachineId_t> activeMachines;
    
    // Track which VMs are migrating
    std::set<VMId_t> migratingVMs;
    
    // Lists of VMs and machines
    std::vector<VMId_t> vms;
    std::vector<MachineId_t> machines;

    // Cache VM and machine information to reduce API calls
    std::map<VMId_t, VMInfo_t> vmInfoCache;
    std::map<MachineId_t, MachineInfo_t> machineInfoCache;
    Time_t lastCacheUpdate = 0;
    
    // Update cache periodically
    void UpdateCaches(Time_t now) {
        // Only update every second
        if (now - lastCacheUpdate < 1000000) return;
        
        vmInfoCache.clear();
        machineInfoCache.clear();
        
        // Cache machine info
        for (MachineId_t machine : machines) {
            try {
                machineInfoCache[machine] = Machine_GetInfo(machine);
            } catch (...) {
                // Skip if error
            }
        }
        
        // Cache VM info
        for (VMId_t vm : vms) {
            try {
                vmInfoCache[vm] = VM_GetInfo(vm);
            } catch (...) {
                // Skip if error
            }
        }
        
        lastCacheUpdate = now;
    }
    
    // Helper methods to access cached info
    MachineInfo_t GetMachineInfo(MachineId_t machine) {
        auto it = machineInfoCache.find(machine);
        if (it != machineInfoCache.end()) {
            return it->second;
        }
        return Machine_GetInfo(machine);
    }
    
    VMInfo_t GetVMInfo(VMId_t vm) {
        auto it = vmInfoCache.find(vm);
        if (it != vmInfoCache.end()) {
            return it->second;
        }
        return VM_GetInfo(vm);
    }
    // Precomputed mappings to avoid repeated searches
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    std::map<MachineId_t, std::vector<VMId_t>> vmsByMachine;
    std::map<SLAType_t, std::vector<TaskId_t>> tasksBySLA;
    
    // Update these mappings in PeriodicCheck
    void UpdateMappings() {
        machinesByCPU.clear();
        vmsByMachine.clear();
        tasksBySLA.clear();
        
        // Group machines by CPU type
        for (MachineId_t machine : machines) {
            try {
                MachineInfo_t info = GetMachineInfo(machine);
                machinesByCPU[info.cpu].push_back(machine);
            } catch (...) {
                // Skip if error
            }
        }
        
        // Group VMs by machine
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t info = GetVMInfo(vm);
                vmsByMachine[info.machine_id].push_back(vm);
                
                // Group tasks by SLA
                for (TaskId_t task : info.active_tasks) {
                    SLAType_t sla = RequiredSLA(task);
                    tasksBySLA[sla].push_back(task);
                }
            } catch (...) {
                // Skip if error
            }
        }
    }
};

#endif /* Scheduler_hpp */