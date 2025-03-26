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
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machinesByCPU[cpuType].push_back(machineId);
    }
    
    // Create more VMs initially to handle the workload better
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        if (machinesWithCPU.empty()) continue;
        
        // Create more VMs initially - up to 8 per CPU type
        unsigned numVMsToCreate = std::min(static_cast<unsigned>(machinesWithCPU.size()), 8u);
        for (unsigned i = 0; i < numVMsToCreate; i++) {
            VMId_t vm = VM_Create(LINUX, cpuType);
            vms.push_back(vm);
            MachineId_t machine = machinesWithCPU[i % machinesWithCPU.size()];
            VM_Attach(vm, machine);
            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;
        }
    }
    
    // Power down unused machines to save energy
    for (MachineId_t machine : machines) {
        if (activeMachines.find(machine) == activeMachines.end()) Machine_SetState(machine, S5);
    }
}

bool Scheduler::SafeRemoveTask(VMId_t vm, TaskId_t task) {
    if (IsVMMigrating(vm)) return false;
    try {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.machine_id == MachineId_t(-1)) return false;
        bool taskFound = false;
        for (TaskId_t t : info.active_tasks) {
            if (t == task) {
                taskFound = true;
                break;
            }
        }
        if (!taskFound) return false;
        if (!IsVMMigrating(vm)) {
            VM_RemoveTask(vm, task);
            return true;
        }
        return false;
    } 
    catch (...) {
        SimOutput("SafeRemoveTask CAUGHT",4);
        return false;
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    
    // Prioritize SLA0 and SLA1 tasks with HIGH_PRIORITY
    Priority_t priority;
    switch (sla_type) {
    case SLA0:
        priority = HIGH_PRIORITY;
        break;
    case SLA1:
        priority = MID_PRIORITY;
        break;
    case SLA2:
        priority = LOW_PRIORITY;
        break;
    case SLA3:
    default:
        priority = LOW_PRIORITY;
        break;
    }
    
    // Find the best VM for this task
    VMId_t target_vm = VMId_t(-1);
    unsigned lowest_task_count = UINT_MAX;
    
    // For SLA0 and SLA1, find VM with fewest tasks
    if (sla_type == SLA0 || sla_type == SLA1) {
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t info = VM_GetInfo(vm);
                if (info.cpu == required_cpu) {
                    // Prefer empty VMs for high-priority tasks
                    if (info.active_tasks.empty()) {
                        target_vm = vm;
                        break;
                    }
                    else if (info.active_tasks.size() < lowest_task_count) {
                        lowest_task_count = info.active_tasks.size();
                        target_vm = vm;
                    }
                }
            } catch (...) {
                SimOutput("NewTask 1 CAUGHT",4);
                continue;
            }
        }
    } 
    // For other SLA types, find any suitable VM
    else {
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t info = VM_GetInfo(vm);
                if (info.cpu == required_cpu) {
                    if (info.active_tasks.size() < lowest_task_count) {
                        lowest_task_count = info.active_tasks.size();
                        target_vm = vm;
                    }
                }
            } catch (...) {
                SimOutput("NewTask 2 CAUGHT",4);
                continue;
            }
        }
    }
    
    // If no suitable VM found, create a new one
    if (target_vm == VMId_t(-1)) {
        MachineId_t target_machine = MachineId_t(-1);
        
        // First check active machines with the required CPU
        for (MachineId_t machine : activeMachines) {
            try {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu == required_cpu) {
                    // For SLA0/SLA1, prefer machines with fewer tasks
                    if (sla_type == SLA0 || sla_type == SLA1) {
                        if (info.active_tasks < 2) {
                            target_machine = machine;
                            break;
                        }
                    } else {
                        target_machine = machine;
                        break;
                    }
                }
            } catch (...) {
                SimOutput("NewTask 3 CAUGHT",4);
                continue;
            }
        }
        
        // If no suitable active machine, power on an inactive one
        if (target_machine == MachineId_t(-1)) {
            for (MachineId_t machine : machines) {
                if (activeMachines.find(machine) != activeMachines.end()) continue;
                
                try {
                    MachineInfo_t info = Machine_GetInfo(machine);
                    if (info.cpu == required_cpu) {
                        Machine_SetState(machine, S0);
                        activeMachines.insert(machine);
                        machineUtilization[machine] = 0.0;
                        target_machine = machine;
                        break;
                    }
                } catch (...) {
                    SimOutput("NewTask 4 CAUGHT",4);
                    continue;
                }
            }
        }
        
        // Create a new VM on the target machine
        if (target_machine != MachineId_t(-1)) {
            try {
                target_vm = VM_Create(required_vm, required_cpu);
                VM_Attach(target_vm, target_machine);
                vms.push_back(target_vm);
                
                // For SLA0/SLA1, set cores to maximum performance immediately
                if (sla_type == SLA0 || sla_type == SLA1) {
                    MachineInfo_t machineInfo = Machine_GetInfo(target_machine);
                    for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                        Machine_SetCorePerformance(target_machine, i, P0);
                    }
                }
            } catch (...) {
                SimOutput("NewTask 5 CAUGHT",4);
            }
        }
    }
    
    // Assign task to VM
    if (target_vm != VMId_t(-1)) {
        if (IsVMMigrating(target_vm)) return;
        
        try {
            VMInfo_t info = VM_GetInfo(target_vm);
            if (info.machine_id == MachineId_t(-1)) return;
            
            VM_AddTask(target_vm, task_id, priority);
            
            // For SLA0/SLA1, ensure the machine is at maximum performance
            if (sla_type == SLA0 || sla_type == SLA1) {
                MachineId_t machine = info.machine_id;
                MachineInfo_t machineInfo = Machine_GetInfo(machine);
                for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P0);
                }
            }
        } catch (...) {
            SimOutput("NewTask 6 CAUGHT",4);
        }
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Update machine utilization
    for (MachineId_t machine : machines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            double utilization = 0.0;
            if (info.num_cpus > 0) {
                utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
            }
            machineUtilization[machine] = utilization;
        } catch (...) {
            SimOutput("Periodic 1 CAUGHT",4);
            continue;
        }
    }
    
    // Adjust CPU performance based on load and SLA
    for (MachineId_t machine : activeMachines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            
            // Check for SLA0/SLA1 tasks on this machine
            bool hasHighPriorityTasks = false;
            
            for (VMId_t vm : vms) {
                if (IsVMMigrating(vm)) continue;
                
                try {
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id != machine) continue;
                    
                    for (TaskId_t task : vmInfo.active_tasks) {
                        SLAType_t slaType = RequiredSLA(task);
                        if (slaType == SLA0 || slaType == SLA1) {
                            hasHighPriorityTasks = true;
                            break;
                        }
                    }
                    
                    if (hasHighPriorityTasks) break;
                } catch (...) {
                    SimOutput("Periodic 2 CAUGHT",4);
                    continue;
                }
            }
            
            // Set CPU performance based on tasks
            if (hasHighPriorityTasks) {
                // Maximum performance for machines with high-priority tasks
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P0);
                }
            }
            else if (info.active_tasks > 0) {
                // For machines with only lower-priority tasks, use P0 if utilization is high
                if (machineUtilization[machine] > 0.5) {
                    for (unsigned i = 0; i < info.num_cpus; i++) {
                        Machine_SetCorePerformance(machine, i, P0);
                    }
                } else {
                    // Use P1 for moderate utilization
                    for (unsigned i = 0; i < info.num_cpus; i++) {
                        Machine_SetCorePerformance(machine, i, P1);
                    }
                }
            }
            else {
                // No active tasks, use lowest performance
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P3);
                }
            }
        } catch (...) {
            SimOutput("Periodic 3 CAUGHT",4);
            continue;
        }
    }
    
    // Skip VM consolidation if there are any migrations in progress
    if (now > 1000000 && migratingVMs.empty()) {
        // Identify underutilized machines
        std::vector<MachineId_t> underutilizedMachines;
        for (MachineId_t machine : activeMachines) {
            try {
                if (machineUtilization[machine] < UNDERLOAD_THRESHOLD) {
                    underutilizedMachines.push_back(machine);
                }
            } catch (...) {
                SimOutput("Periodic 4 CAUGHT",4);
                continue;
            }
        }
        
        // Process one underutilized machine at a time
        if (!underutilizedMachines.empty()) {
            MachineId_t sourceMachine = underutilizedMachines[0];
            
            try {
                MachineInfo_t sourceInfo = Machine_GetInfo(sourceMachine);
                
                // Check if this machine has any high-priority tasks
                bool hasHighPriorityTasks = false;
                for (VMId_t vm : vms) {
                    if (IsVMMigrating(vm)) continue;
                    
                    try {
                        VMInfo_t vmInfo = VM_GetInfo(vm);
                        if (vmInfo.machine_id != sourceMachine) continue;
                        
                        for (TaskId_t task : vmInfo.active_tasks) {
                            SLAType_t slaType = RequiredSLA(task);
                            if (slaType == SLA0 || slaType == SLA1) {
                                hasHighPriorityTasks = true;
                                break;
                            }
                        }
                        
                        if (hasHighPriorityTasks) break;
                    } catch (...) {
                        SimOutput("Periodic 5 CAUGHT",4);
                        continue;
                    }
                }
                
                // Skip migration if this machine has high-priority tasks
                if (!hasHighPriorityTasks && sourceInfo.active_vms > 0) {
                    // Find a VM to migrate
                    VMId_t vmToMigrate = VMId_t(-1);
                    
                    for (VMId_t vm : vms) {
                        if (IsVMMigrating(vm)) continue;
                        
                        try {
                            VMInfo_t vmInfo = VM_GetInfo(vm);
                            if (vmInfo.machine_id != sourceMachine) continue;
                            
                            // Check if this VM has any high-priority tasks
                            bool vmHasHighPriorityTasks = false;
                            for (TaskId_t task : vmInfo.active_tasks) {
                                SLAType_t slaType = RequiredSLA(task);
                                if (slaType == SLA0 || slaType == SLA1) {
                                    vmHasHighPriorityTasks = true;
                                    break;
                                }
                            }
                            
                            // Skip VMs with high-priority tasks
                            if (!vmHasHighPriorityTasks) {
                                vmToMigrate = vm;
                                break;
                            }
                        } catch (...) {
                            SimOutput("Periodic 6 CAUGHT",4);
                            continue;
                        }
                    }
                    
                    // Migrate the VM if found
                    if (vmToMigrate != VMId_t(-1)) {
                        try {
                            VMInfo_t vmInfo = VM_GetInfo(vmToMigrate);
                            CPUType_t vmCpuType = vmInfo.cpu;
                            
                            // Find a suitable target machine
                            MachineId_t targetMachine = MachineId_t(-1);
                            double bestUtilization = 0.0;
                            
                            for (MachineId_t machine : activeMachines) {
                                if (machine == sourceMachine) continue;
                                
                                try {
                                    MachineInfo_t machineInfo = Machine_GetInfo(machine);
                                    if (machineInfo.cpu == vmCpuType && 
                                        machineUtilization[machine] > bestUtilization && 
                                        machineUtilization[machine] < OVERLOAD_THRESHOLD) {
                                        
                                        targetMachine = machine;
                                        bestUtilization = machineUtilization[machine];
                                    }
                                } catch (...) {
                                    SimOutput("Periodic 7 CAUGHT",4);
                                    continue;
                                }
                            }
                            
                            // Perform the migration
                            if (targetMachine != MachineId_t(-1)) {
                                MarkVMAsMigrating(vmToMigrate);
                                VM_Migrate(vmToMigrate, targetMachine);
                            }
                        } catch (...) {
                            SimOutput("Periodic 8 CAUGHT",4);
                            MarkVMAsReady(vmToMigrate);
                        }
                    }
                }
            } catch (...) {
                SimOutput("Periodic 9 CAUGHT",4);
            }
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Update machine utilization
    for (MachineId_t machine : machines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            double utilization = 0.0;
            if (info.num_cpus > 0) {
                utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
            }
            machineUtilization[machine] = utilization;
        } catch (...) {
            SimOutput("Task Complete 1 CAUGHT",4);
            continue;
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    MarkVMAsReady(vm_id);
}

void InitScheduler() {
    std::cout << "DIRECT OUTPUT: InitScheduler starting" << std::endl;
    std::cout.flush();
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id){
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    try {
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        
        // Identify VMs on this machine
        std::vector<VMId_t> vmsOnMachine;
        std::map<VMId_t, unsigned> vmTaskCount;
        
        for (VMId_t vm : scheduler.GetVMs()) {
            if (scheduler.IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t vmInfo = VM_GetInfo(vm);
                if (vmInfo.machine_id == machine_id) {
                    vmsOnMachine.push_back(vm);
                    vmTaskCount[vm] = vmInfo.active_tasks.size();
                }
            } catch (...) {
                SimOutput("MemoryWarning 1 CAUGHT",4);
                continue;
            }
        }
        
        // Find the VM with the most tasks
        VMId_t largestVM = VMId_t(-1);
        unsigned mostTasks = 0;
        
        for (const auto &pair : vmTaskCount) {
            if (pair.second > mostTasks) {
                mostTasks = pair.second;
                largestVM = pair.first;
            }
        }
        
        // Migrate the VM if possible
        if (largestVM != VMId_t(-1) && !scheduler.IsVMMigrating(largestVM)) {
            try {
                VMInfo_t vmInfo = VM_GetInfo(largestVM);
                CPUType_t vmCpuType = vmInfo.cpu;
                
                // Check for high-priority tasks
                bool hasHighPriorityTasks = false;
                for (TaskId_t task : vmInfo.active_tasks) {
                    SLAType_t slaType = RequiredSLA(task);
                    if (slaType == SLA0 || slaType == SLA1) {
                        hasHighPriorityTasks = true;
                        break;
                    }
                }
                
                // Find a suitable target machine
                MachineId_t targetMachine = MachineId_t(-1);
                
                for (MachineId_t machine : scheduler.GetMachines()) {
                    if (machine == machine_id) continue;
                    if (!scheduler.IsMachineActive(machine)) continue;
                    MachineInfo_t info = Machine_GetInfo(machine);
                    if (info.s_state != S0) continue;
                    try {
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.cpu != vmCpuType) continue;
                        
                        // For VMs with high-priority tasks, prefer machines with no tasks
                        if (hasHighPriorityTasks) {
                            if (info.active_tasks == 0) {
                                targetMachine = machine;
                                break;
                            }
                        } else if (info.active_tasks < 2) {
                            targetMachine = machine;
                            break;
                        }
                    } catch (...) {
                        SimOutput("MemoryWarning 2 CAUGHT",4);
                        continue;
                    }
                }
                
                // If no suitable active machine, power on a new one
                if (targetMachine == MachineId_t(-1)) {
                    for (MachineId_t machine : scheduler.GetMachines()) {
                        if (scheduler.IsMachineActive(machine)) continue;
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.s_state != S0) continue;
                        try {
                            MachineInfo_t info = Machine_GetInfo(machine);
                            if (info.cpu != vmCpuType) continue;
                            
                            Machine_SetState(machine, S0);
                            scheduler.ActivateMachine(machine);
                            targetMachine = machine;
                            break;
                        } catch (...) {
                            SimOutput("MemoryWarning 3 CAUGHT",4);
                            continue;
                        }
                    }
                }
                
                // Perform the migration
                if (targetMachine != MachineId_t(-1)) {
                    scheduler.MarkVMAsMigrating(largestVM);
                    VM_Migrate(largestVM, targetMachine);
                } else {
                    // If migration not possible, power on another machine as a last resort
                    for (MachineId_t machine : scheduler.GetMachines()) {
                        if (scheduler.IsMachineActive(machine)) continue;
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.s_state != S0) continue;
                        try {
                            Machine_SetState(machine, S0);
                            scheduler.ActivateMachine(machine);
                            break;
                        } catch (...) {
                            SimOutput("MemoryWarning 4 CAUGHT",4);
                            continue;
                        }
                    }
                }
            } catch (...) {
                SimOutput("MemoryWarning 5 CAUGHT",4);
                scheduler.MarkVMAsReady(largestVM);
            }
        }
        
        // Set all cores to maximum performance to help process tasks faster
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(machine_id, i, P0);
        }
    } catch (...) {
        SimOutput("MemoryWarning 6 CAUGHT",4);
    }
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("Migration done for vm " + vm_id,4);
    scheduler.MigrationComplete(time, vm_id);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    try {
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        MachineState_t currentState = machineInfo.s_state;
        
        if (currentState == S0) {
            scheduler.ActivateMachine(machine_id);
            
            // Set cores to appropriate performance state
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(machine_id, i, P1);
            }
            
            // Check if this machine already has a VM
            bool hasVM = false;
            for (VMId_t vm : scheduler.GetVMs()) {
                if (scheduler.IsVMMigrating(vm)) continue;
                
                try {
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id == machine_id) {
                        hasVM = true;
                        break;
                    }
                } catch (...) {
                    SimOutput("StateChangeComplete 1 CAUGHT",4);
                    continue;
                }
            }
            
            // Create a VM if needed
            if (!hasVM) {
                try {
                    VMId_t newVM = VM_Create(LINUX, machineInfo.cpu);
                    VM_Attach(newVM, machine_id);
                    scheduler.AddVM(newVM);
                } catch (...) {
                    SimOutput("StateChangeComplete 2 CAUGHT",4);
                }
            }
        }
        else if (currentState == S5) {
            scheduler.DeactivateMachine(machine_id);
        }
    } catch (...) {
        SimOutput("StateChangeComplete 3 CAUGHT",4);
    }
    
    // Run periodic check to update system state
    scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SLAType_t slaType = RequiredSLA(task_id);
    
    // Find which VM is running this task
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);
    
    for (VMId_t vm : scheduler.GetVMs()) {
        if (scheduler.IsVMMigrating(vm)) continue;
        
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == MachineId_t(-1)) continue;
            for (TaskId_t task : vmInfo.active_tasks) {
                if (task == task_id) {
                    taskVM = vm;
                    taskMachine = vmInfo.machine_id;
                    break;
                }
            }
            
            if (taskVM != VMId_t(-1)) break;
        } catch (...) {
            SimOutput("SLAWarning 1 CAUGHT",4);
            continue;
        }
    }
    
    // Take action based on SLA type
    if (taskVM != VMId_t(-1)) {
        // For SLA0 and SLA1, take immediate action
        if (slaType == SLA0 || slaType == SLA1) {
            // Set task to highest priority
            SetTaskPriority(task_id, HIGH_PRIORITY);
            
            try {
                // Set machine to maximum performance
                MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
                for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                    Machine_SetCorePerformance(taskMachine, i, P0);
                }
                
                if (!scheduler.IsVMMigrating(taskVM)) {

                    // Move other tasks away from this VM if possible

                    VMInfo_t vmInfo = VM_GetInfo(taskVM);

                    

                    std::vector<TaskId_t> tasksToMove;

                    for (TaskId_t otherTask : vmInfo.active_tasks) {

                        if (otherTask == task_id) continue;

                        

                        SLAType_t otherSlaType = RequiredSLA(otherTask);

                        if (otherSlaType != SLA0 && otherSlaType != SLA1) {

                            tasksToMove.push_back(otherTask);

                        }

                    }

                    

                    // Process one task at a time

                    if (!tasksToMove.empty()) {

                        TaskId_t otherTask = tasksToMove[0];

                        
                        // Find another VM for this lower-priority task
                        VMId_t targetVM = VMId_t(-1);
                        
                        for (VMId_t vm : scheduler.GetVMs()) {
                            if (vm == taskVM || scheduler.IsVMMigrating(vm)) continue;
                            
                            try {
                                VMInfo_t otherVMInfo = VM_GetInfo(vm);
                                if (otherVMInfo.machine_id == MachineId_t(-1)) continue;
                                if (otherVMInfo.cpu == vmInfo.cpu && otherVMInfo.active_tasks.size() < 3) {
                                    targetVM = vm;
                                    break;
                                }
                            } catch (...) {
                                SimOutput("SLAWarning 2 CAUGHT",4);
                                continue;
                            }
                        }
                        
                        // Move the task if a suitable VM was found
                        if (targetVM != VMId_t(-1)) {
                            if (scheduler.SafeRemoveTask(taskVM, otherTask)) {
                                try {

                                    VM_AddTask(targetVM, otherTask, MID_PRIORITY);

                                } catch (...) {
                                    SimOutput("SLAWarning 3 CAUGHT",4);
                                }
                            }
                        }
                    }
                }
            } catch (...) {
                SimOutput("SLAWarning 4 CAUGHT",4);
            }
        }
        // For SLA2, just increase priority
        else if (slaType == SLA2) {
            SetTaskPriority(task_id, MID_PRIORITY);
        }
    }
}