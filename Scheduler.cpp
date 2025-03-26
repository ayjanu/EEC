//
//  Scheduler.cpp
//  CloudSim
//
#include "Scheduler.hpp"
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <climits>

// Global Scheduler instance
static Scheduler scheduler;

void Scheduler::Init() {
    migratingVMs.clear();
    unsigned totalMachines = Machine_GetTotal();
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    
    // Categorize machines by CPU type
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machinesByCPU[cpuType].push_back(machineId);
    }
    
    // Create VMs for each CPU type
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        if (machinesWithCPU.empty()) continue;
        
        unsigned numVMsToCreate = std::min(static_cast<unsigned>(machinesWithCPU.size()), 4u);
        for (unsigned i = 0; i < numVMsToCreate; i++) {
            VMId_t vm = VM_Create(LINUX, cpuType);
            vms.push_back(vm);
            MachineId_t machine = machinesWithCPU[i % machinesWithCPU.size()];
            VM_Attach(vm, machine);
            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;
        }
    }
    
    // Power down unused machines
    for (MachineId_t machine : machines) {
        if (activeMachines.find(machine) == activeMachines.end()) 
            Machine_SetState(machine, S5);
    }
}

bool Scheduler::SafeRemoveTask(VMId_t vm, TaskId_t task) {
    if (IsVMMigrating(vm)) return false;
    try {
        VMInfo_t info = VM_GetInfo(vm);
        for (TaskId_t t : info.active_tasks) {
            if (t == task) {
                VM_RemoveTask(vm, task);
                return true;
            }
        }
        return false;
    } 
    catch (...) {
        return false;
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    
    Priority_t priority;
    switch (sla_type) {
    case SLA0:
        priority = HIGH_PRIORITY;
        break;
    case SLA1:
        priority = MID_PRIORITY;
        break;
    case SLA2:
        priority = MID_PRIORITY;
        break;
    case SLA3:
    default:
        priority = LOW_PRIORITY;
        break;
    }
    
    VMId_t target_vm = VMId_t(-1);
    
    // For SLA0 tasks, try to find a VM with only SLA0 tasks or no tasks
    if (sla_type == SLA0) {
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            VMInfo_t info = VM_GetInfo(vm);
            if (info.cpu != required_cpu) continue;
            
            // Check if VM has only SLA0 tasks
            bool onlySLA0Tasks = true;
            for (TaskId_t existingTask : info.active_tasks) {
                if (RequiredSLA(existingTask) != SLA0) {
                    onlySLA0Tasks = false;
                    break;
                }
            }
            
            if (onlySLA0Tasks) {
                // Prefer VMs with fewer tasks
                if (target_vm == VMId_t(-1) || info.active_tasks.size() < VM_GetInfo(target_vm).active_tasks.size()) {
                    target_vm = vm;
                }
            }
        }
    }
    
    // If no dedicated VM found for SLA0 or for other SLA types, use regular approach
    if (target_vm == VMId_t(-1)) {
        unsigned lowest_task_count = UINT_MAX;
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            VMInfo_t info = VM_GetInfo(vm);
            if (info.cpu == required_cpu) {
                if (sla_type == SLA0) {
                    if (info.active_tasks.size() < lowest_task_count) {
                        lowest_task_count = info.active_tasks.size();
                        target_vm = vm;
                    }
                } else {
                    target_vm = vm;
                    break;
                }
            }
        }
    }
    
    // If still no suitable VM, create a new one
    if (target_vm == VMId_t(-1)) {
        MachineId_t target_machine = MachineId_t(-1);
        
        // For SLA0, try to find a machine with low utilization
        if (sla_type == SLA0) {
            double lowest_util = 1.0;
            for (MachineId_t machine : activeMachines) {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu != required_cpu) continue;
                
                double util = machineUtilization[machine];
                if (util < lowest_util) {
                    lowest_util = util;
                    target_machine = machine;
                }
            }
        }
        
        // If no suitable machine found or not SLA0, use regular approach
        if (target_machine == MachineId_t(-1)) {
            for (MachineId_t machine : activeMachines) {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu == required_cpu) {
                    target_machine = machine;
                    break;
                }
            }
        }
        
        // If still no suitable machine, power on a new one
        if (target_machine == MachineId_t(-1)) {
            for (MachineId_t machine : machines) {
                if (activeMachines.find(machine) != activeMachines.end()) continue;
                
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu == required_cpu) {
                    Machine_SetState(machine, S0);
                    activeMachines.insert(machine);
                    machineUtilization[machine] = 0.0;
                    target_machine = machine;
                    break;
                }
            }
        }
        
        // Create a new VM on the target machine
        if (target_machine != MachineId_t(-1)) {
            target_vm = VM_Create(required_vm, required_cpu);
            VM_Attach(target_vm, target_machine);
            vms.push_back(target_vm);
            
            // For SLA0, set all cores to maximum performance immediately
            if (sla_type == SLA0) {
                MachineInfo_t machineInfo = Machine_GetInfo(target_machine);
                for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                    Machine_SetCorePerformance(target_machine, i, P0);
                }
            }
        }
    }
    
    // Assign task to VM
    if (target_vm != VMId_t(-1)) {
        if (IsVMMigrating(target_vm)) return;
        VM_AddTask(target_vm, task_id, priority);
    }
}

void Scheduler::OptimizeForSLA0Tasks() {
    // Find all machines with SLA0 tasks
    std::set<MachineId_t> machinesWithSLA0;
    
    for (VMId_t vm : vms) {
        if (IsVMMigrating(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        bool hasSLA0 = false;
        
        for (TaskId_t task : vmInfo.active_tasks) {
            if (RequiredSLA(task) == SLA0) {
                hasSLA0 = true;
                break;
            }
        }
        
        if (hasSLA0) {
            machinesWithSLA0.insert(vmInfo.machine_id);
        }
    }
    
    // Set all cores to maximum performance on machines with SLA0 tasks
    for (MachineId_t machine : machinesWithSLA0) {
        MachineInfo_t info = Machine_GetInfo(machine);
        
        for (unsigned i = 0; i < info.num_cpus; i++) {
            Machine_SetCorePerformance(machine, i, P0);
        }
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Update machine utilization
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        double utilization = 0.0;
        if (info.num_cpus > 0)
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        machineUtilization[machine] = utilization;
    }
    
    // Optimize for SLA0 tasks
    OptimizeForSLA0Tasks();
    
    // Set performance states for machines without SLA0 tasks
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        
        // Check if this machine has any SLA0 tasks
        bool hasSLA0 = false;
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine) continue;
            
            for (TaskId_t task : vmInfo.active_tasks) {
                if (RequiredSLA(task) == SLA0) {
                    hasSLA0 = true;
                    break;
                }
            }
            
            if (hasSLA0) break;
        }
        
        // Only adjust performance for machines without SLA0 tasks
        if (!hasSLA0) {
            if (info.active_tasks > 0) {
                for (unsigned i = 0; i < info.num_cpus; i++)
                    Machine_SetCorePerformance(machine, i, P0);
            } else {
                for (unsigned i = 0; i < info.num_cpus; i++)
                    Machine_SetCorePerformance(machine, i, P3);
            }
        }
    }
    
    // Monitor SLA0 tasks to prevent violations
    MonitorSLA0Tasks(now);
    
    // VM consolidation (only after initial startup phase)
    if (now > 1000000) {
        std::vector<MachineId_t> underutilizedMachines;
        for (MachineId_t machine : activeMachines) {
            if (machineUtilization[machine] < UNDERLOAD_THRESHOLD) {
                // Check if this machine has any SLA0 tasks
                bool hasSLA0 = false;
                for (VMId_t vm : vms) {
                    if (IsVMMigrating(vm)) continue;
                    
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id != machine) continue;
                    
                    for (TaskId_t task : vmInfo.active_tasks) {
                        if (RequiredSLA(task) == SLA0) {
                            hasSLA0 = true;
                            break;
                        }
                    }
                    
                    if (hasSLA0) break;
                }
                
                // Only consider consolidating machines without SLA0 tasks
                if (!hasSLA0) {
                    underutilizedMachines.push_back(machine);
                }
            }
        }
        
        if (!underutilizedMachines.empty()) {
            MachineId_t sourceMachine = underutilizedMachines[0];
            MachineInfo_t sourceInfo = Machine_GetInfo(sourceMachine);
            
            if (sourceInfo.active_vms > 0) {
                VMId_t vmToMigrate = VMId_t(-1);
                
                // Find a VM without SLA0 tasks
                for (VMId_t vm : vms) {
                    if (IsVMMigrating(vm)) continue;
                    
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id != sourceMachine) continue;
                    
                    bool hasSLA0 = false;
                    for (TaskId_t task : vmInfo.active_tasks) {
                        if (RequiredSLA(task) == SLA0) {
                            hasSLA0 = true;
                            break;
                        }
                    }
                    
                    if (!hasSLA0) {
                        vmToMigrate = vm;
                        break;
                    }
                }
                
                // If no VM without SLA0 tasks found, use any VM
                if (vmToMigrate == VMId_t(-1)) {
                    for (VMId_t vm : vms) {
                        VMInfo_t vmInfo = VM_GetInfo(vm);
                        if (vmInfo.machine_id == sourceMachine) {
                            vmToMigrate = vm;
                            break;
                        }
                    }
                }
                
                if (vmToMigrate != VMId_t(-1) && !IsVMMigrating(vmToMigrate)) {
                    VMInfo_t vmInfo = VM_GetInfo(vmToMigrate);
                    CPUType_t vmCpuType = vmInfo.cpu;
                    MachineId_t targetMachine = MachineId_t(-1);
                    double bestUtilization = 0.0;
                    
                    for (MachineId_t machine : activeMachines) {
                        if (machine == sourceMachine) continue;
                        
                        MachineInfo_t machineInfo = Machine_GetInfo(machine);
                        if (machineInfo.cpu == vmCpuType && 
                            machineUtilization[machine] > bestUtilization && 
                            machineUtilization[machine] < OVERLOAD_THRESHOLD) {
                            targetMachine = machine;
                            bestUtilization = machineUtilization[machine];
                        }
                    }
                    
                    if (targetMachine != MachineId_t(-1)) {
                        MarkVMAsMigrating(vmToMigrate);
                        VM_Migrate(vmToMigrate, targetMachine);
                    }
                }
            }
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Update machine utilization
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        double utilization = 0.0;
        if (info.num_cpus > 0) 
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        machineUtilization[machine] = utilization;
    }
}

void Scheduler::Shutdown(Time_t time) {
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
}

void Scheduler::MonitorSLA0Tasks(Time_t now) {
    // Find all SLA0 tasks
    std::vector<std::pair<TaskId_t, VMId_t>> sla0Tasks;
    
    for (VMId_t vm : vms) {
        if (IsVMMigrating(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        for (TaskId_t task : vmInfo.active_tasks) {
            if (RequiredSLA(task) == SLA0) {
                sla0Tasks.push_back(std::make_pair(task, vm));
            }
        }
    }
    
    // For each SLA0 task, check if it's at risk
    for (const auto& pair : sla0Tasks) {
        TaskId_t task = pair.first;
        VMId_t vm = pair.second;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        MachineId_t machine = vmInfo.machine_id;
        MachineInfo_t machineInfo = Machine_GetInfo(machine);
        
        // If the machine has more than 2 tasks per CPU, consider the SLA0 task at risk
        if (machineInfo.active_tasks > machineInfo.num_cpus * 2) {
            // Ensure this task has HIGH_PRIORITY
            SetTaskPriority(task, HIGH_PRIORITY);
            
            // Set machine to maximum performance
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P0);
            }
            
            // If there are multiple tasks on this VM, try to move non-SLA0 tasks
            if (vmInfo.active_tasks.size() > 1) {
                for (TaskId_t otherTask : vmInfo.active_tasks) {
                    if (otherTask != task && RequiredSLA(otherTask) != SLA0) {
                        // Find another VM for this task
                        VMId_t targetVM = VMId_t(-1);
                        for (VMId_t otherVM : vms) {
                            if (otherVM == vm || IsVMMigrating(otherVM)) continue;
                            
                            VMInfo_t otherVMInfo = VM_GetInfo(otherVM);
                            if (otherVMInfo.cpu == vmInfo.cpu) {
                                targetVM = otherVM;
                                break;
                            }
                        }
                        
                        if (targetVM != VMId_t(-1)) {
                            // Move the non-SLA0 task to another VM
                            Priority_t priority;
                            SLAType_t otherSlaType = RequiredSLA(otherTask);
                            switch (otherSlaType) {
                                case SLA1: priority = MID_PRIORITY; break;
                                case SLA2: priority = LOW_PRIORITY; break;
                                default: priority = LOW_PRIORITY; break;
                            }
                            
                            if (SafeRemoveTask(vm, otherTask)) {
                                VM_AddTask(targetVM, otherTask, priority);
                                break; // Just move one task for now
                            }
                        }
                    }
                }
            }
        }
    }
}

void InitScheduler() {
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
    std::vector<VMId_t> vmsOnMachine;
    std::map<VMId_t, unsigned> vmTaskCount;
    
    // Find VMs on this machine and count their tasks
    for (VMId_t vm : scheduler.GetVMs()) {
        if (scheduler.IsVMMigrating(vm)) continue;
        
        VMInfo_t vmInfo;
        try {
            vmInfo = VM_GetInfo(vm);
        } catch (...) {
            continue;
        }
        
        if (vmInfo.machine_id == machine_id) {
            vmsOnMachine.push_back(vm);
            vmTaskCount[vm] = vmInfo.active_tasks.size();
        }
    }
    
    // Find VM with most tasks, prioritizing those without SLA0 tasks
    VMId_t largestVM = VMId_t(-1);
    unsigned mostTasks = 0;
    
    // First try to find a VM without SLA0 tasks
    for (const auto &pair : vmTaskCount) {
        VMId_t vm = pair.first;
        unsigned taskCount = pair.second;
        
        if (taskCount <= mostTasks) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        bool hasSLA0 = false;
        
        for (TaskId_t task : vmInfo.active_tasks) {
            if (RequiredSLA(task) == SLA0) {
                hasSLA0 = true;
                break;
            }
        }
        
        if (!hasSLA0) {
            mostTasks = taskCount;
            largestVM = vm;
        }
    }
    
    // If no VM without SLA0 tasks found, use any VM
    if (largestVM == VMId_t(-1)) {
        for (const auto &pair : vmTaskCount) {
            if (pair.second > mostTasks) {
                mostTasks = pair.second;
                largestVM = pair.first;
            }
        }
    }
    
    // Migrate the selected VM
    if (largestVM != VMId_t(-1) && !scheduler.IsVMMigrating(largestVM)) {
        VMInfo_t vmInfo = VM_GetInfo(largestVM);
        CPUType_t vmCpuType = vmInfo.cpu;
        MachineId_t targetMachine = MachineId_t(-1);
        
        // Find a suitable target machine
        for (MachineId_t machine : scheduler.GetMachines()) {
            if (machine == machine_id) continue;
            if (!scheduler.IsMachineActive(machine)) continue;
            
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.cpu != vmCpuType) continue;
            
            if (info.active_tasks < 2) { // Machine has 0 or 1 active tasks
                targetMachine = machine;
                break;
            }
        }
        
        // If no suitable active machine, power on a new one
        if (targetMachine == MachineId_t(-1)) {
            for (MachineId_t machine : scheduler.GetMachines()) {
                if (scheduler.IsMachineActive(machine)) continue;
                
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu != vmCpuType) continue;
                
                Machine_SetState(machine, S0);
                scheduler.ActivateMachine(machine);
                targetMachine = machine;
                break;
            }
        }
        
        // Perform migration
        if (targetMachine != MachineId_t(-1)) {
            scheduler.MarkVMAsMigrating(largestVM);
            VM_Migrate(largestVM, targetMachine);
        } else {
            // If no suitable target found, power on any machine
            for (MachineId_t machine : scheduler.GetMachines()) {
                if (scheduler.IsMachineActive(machine)) continue;
                
                Machine_SetState(machine, S0);
                scheduler.ActivateMachine(machine);
                break;
            }
        }
    }
    
    // Set all cores to maximum performance to help process tasks faster
    for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
        Machine_SetCorePerformance(machine_id, i, P0);
    }
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    scheduler.MarkVMAsReady(vm_id);
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
    MachineState_t currentState = machineInfo.s_state;
    
    if (currentState == S0) {
        scheduler.ActivateMachine(machine_id);
        
        // Set initial performance state
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(machine_id, i, P0);  // Start with high performance
        }
        
        // Create a VM if none exists
        bool hasVM = false;
        for (VMId_t vm : scheduler.GetVMs()) {
            if (scheduler.IsVMMigrating(vm)) continue;
            
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machine_id) {
                hasVM = true;
                break;
            }
        }
        
        if (!hasVM) {
            VMId_t newVM = VM_Create(LINUX, machineInfo.cpu);
            VM_Attach(newVM, machine_id);
            scheduler.AddVM(newVM);
        }
    }
    else if (currentState == S5) {
        scheduler.DeactivateMachine(machine_id);
    }
    
    scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SLAType_t slaType = RequiredSLA(task_id);
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);
    
    // Find which VM is running this task
    for (VMId_t vm : scheduler.GetVMs()) {
        if (scheduler.IsVMMigrating(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        for (TaskId_t task : vmInfo.active_tasks) {
            if (task == task_id) {
                taskVM = vm;
                taskMachine = vmInfo.machine_id;
                break;
            }
        }
        
        if (taskVM != VMId_t(-1)) break;
    }
    
    if (taskVM != VMId_t(-1)) {
        if (slaType == SLA0) {
            // For SLA0, take immediate and aggressive action
            SetTaskPriority(task_id, HIGH_PRIORITY);
            
            // Set machine to maximum performance
            MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(taskMachine, i, P0);
            }
            
            // If the machine is overloaded, try to reduce load
            if (machineInfo.active_tasks > machineInfo.num_cpus) {
                VMInfo_t vmInfo = VM_GetInfo(taskVM);
                
                // First try to move other tasks away from this VM
                for (TaskId_t otherTask : vmInfo.active_tasks) {
                    if (otherTask == task_id || RequiredSLA(otherTask) == SLA0) continue;
                    
                    // Find another VM for this task
                    VMId_t targetVM = VMId_t(-1);
                    for (VMId_t otherVM : scheduler.GetVMs()) {
                        if (otherVM == taskVM || scheduler.IsVMMigrating(otherVM)) continue;
                        
                        VMInfo_t otherVMInfo = VM_GetInfo(otherVM);
                        if (otherVMInfo.cpu == vmInfo.cpu) {
                            targetVM = otherVM;
                            break;
                        }
                    }
                    
                    if (targetVM != VMId_t(-1)) {
                        // Move the non-SLA0 task to another VM
                        Priority_t priority;
                        SLAType_t otherSlaType = RequiredSLA(otherTask);
                        switch (otherSlaType) {
                            case SLA1: priority = MID_PRIORITY; break;
                            case SLA2: priority = LOW_PRIORITY; break;
                            default: priority = LOW_PRIORITY; break;
                        }
                        
                        if (scheduler.SafeRemoveTask(taskVM, otherTask)) {
                            VM_AddTask(targetVM, otherTask, priority);
                            break;
                        }
                    }
                }
                
                // If still overloaded, try to migrate this VM
                MachineInfo_t updatedInfo = Machine_GetInfo(taskMachine);
                if (updatedInfo.active_tasks > updatedInfo.num_cpus) {
                    CPUType_t vmCpuType = vmInfo.cpu;
                    MachineId_t targetMachine = MachineId_t(-1);
                    
                    // Find a less loaded machine
                    for (MachineId_t machine : scheduler.GetMachines()) {
                        if (machine == taskMachine) continue;
                        if (!scheduler.IsMachineActive(machine)) continue;
                        
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.cpu != vmCpuType) continue;
                        
                        if (info.active_tasks < machineInfo.active_tasks / 2) {
                            targetMachine = machine;
                            break;
                        }
                    }
                    
                    // If no suitable active machine, power on a new one
                    if (targetMachine == MachineId_t(-1)) {
                        for (MachineId_t machine : scheduler.GetMachines()) {
                            if (scheduler.IsMachineActive(machine)) continue;
                            
                            MachineInfo_t info = Machine_GetInfo(machine);
                            if (info.cpu != vmCpuType) continue;
                            
                            Machine_SetState(machine, S0);
                            scheduler.ActivateMachine(machine);
                            targetMachine = machine;
                            break;
                        }
                    }
                    
                    // Perform migration
                    if (targetMachine != MachineId_t(-1)) {
                        scheduler.MarkVMAsMigrating(taskVM);
                        VM_Migrate(taskVM, targetMachine);
                    }
                }
            }
        }
        else if (slaType == SLA1) {
            // For SLA1, take action but less aggressive than SLA0
            SetTaskPriority(task_id, HIGH_PRIORITY);
            
            MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(taskMachine, i, P0);
            }
            
            // Only migrate if severely overloaded
            if (machineInfo.active_tasks > machineInfo.num_cpus * 2) {
                // Migration logic similar to SLA0 but with higher threshold
                VMInfo_t vmInfo = VM_GetInfo(taskVM);
                CPUType_t vmCpuType = vmInfo.cpu;
                MachineId_t targetMachine = MachineId_t(-1);
                
                for (MachineId_t machine : scheduler.GetMachines()) {
                    if (machine == taskMachine) continue;
                    if (!scheduler.IsMachineActive(machine)) continue;
                    
                    MachineInfo_t info = Machine_GetInfo(machine);
                    if (info.cpu != vmCpuType) continue;
                    
                    if (info.active_tasks < machineInfo.active_tasks / 2) {
                        targetMachine = machine;
                        break;
                    }
                }
                
                if (targetMachine == MachineId_t(-1)) {
                    for (MachineId_t machine : scheduler.GetMachines()) {
                        if (scheduler.IsMachineActive(machine)) continue;
                        
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.cpu != vmCpuType) continue;
                        
                        Machine_SetState(machine, S0);
                        scheduler.ActivateMachine(machine);
                        targetMachine = machine;
                        break;
                    }
                }
                
                if (targetMachine != MachineId_t(-1)) {
                    scheduler.MarkVMAsMigrating(taskVM);
                    VM_Migrate(taskVM, targetMachine);
                }
            }
        }
        else if (slaType == SLA2) {
            // For SLA2, just increase priority
            SetTaskPriority(task_id, HIGH_PRIORITY);
        }
    }
}