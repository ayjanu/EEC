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
    
    // Track machines by CPU type and GPU capability
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    std::map<CPUType_t, std::vector<MachineId_t>> gpuMachinesByCPU;
    
    // First, categorize all machines
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        
        // Categorize by CPU type and GPU capability
        if (info.gpus) {
            gpuMachinesByCPU[cpuType].push_back(machineId);
        } else {
            machinesByCPU[cpuType].push_back(machineId);
        }
    }
    
    // Create VMs for each CPU type and GPU capability
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        
        if (machinesWithCPU.empty()) continue;
        
        // Create VMs for this CPU type - at least 2 per CPU type
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
    
    // Create VMs for GPU-enabled machines
    for (const auto &pair : gpuMachinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &gpuMachines = pair.second;
        
        if (gpuMachines.empty()) continue;
        
        // Create VMs for GPU-enabled machines - at least 2 per CPU type
        unsigned numVMsToCreate = std::min(static_cast<unsigned>(gpuMachines.size()), 4u);
        
        for (unsigned i = 0; i < numVMsToCreate; i++) {
            VMId_t vm = VM_Create(LINUX, cpuType);
            vms.push_back(vm);
            
            MachineId_t machine = gpuMachines[i % gpuMachines.size()];
            VM_Attach(vm, machine);
            
            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;
        }
    }
    
    // Create AIX VMs for POWER CPUs if available
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        if (info.cpu == POWER && activeMachines.find(machineId) == activeMachines.end()) {
            VMId_t vm = VM_Create(AIX, POWER);
            vms.push_back(vm);
            VM_Attach(vm, machineId);
            
            activeMachines.insert(machineId);
            machineUtilization[machineId] = 0.0;
            break; // Just create one AIX VM
        }
    }
    
    // Power down unused machines to save energy
    for (MachineId_t machine : machines) {
        if (activeMachines.find(machine) == activeMachines.end()) {
            Machine_SetState(machine, S5);
        }
    }
}

bool Scheduler::SafeRemoveTask(VMId_t vm, TaskId_t task) {
    // First check if VM is migrating
    if (IsVMMigrating(vm)) {
        return false;
    }
    
    try {
        // Get VM info and validate
        VMInfo_t info = VM_GetInfo(vm);
        
        // Check if VM is attached to a machine
        if (info.machine_id == MachineId_t(-1)) {
            return false;
        }
        
        // Check if the machine is active
        if (activeMachines.find(info.machine_id) == activeMachines.end()) {
            return false;
        }
        
        // Check if task exists in this VM
        bool taskFound = false;
        for (TaskId_t t : info.active_tasks) {
            if (t == task) {
                taskFound = true;
                break;
            }
        }
        
        if (!taskFound) {
            return false;
        }
        
        // Double-check VM state before removing task
        if (!IsVMMigrating(vm)) {
            VM_RemoveTask(vm, task);
            return true;
        }
        
        return false;
    } 
    catch (...) {
        return false;
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task requirements
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    bool gpu_required = IsTaskGPUCapable(task_id);
    
    // Assign priority based on SLA
    Priority_t priority;
    switch (sla_type) {
    case SLA0:
        priority = MID_PRIORITY;
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
    
    // Find a suitable VM
    VMId_t target_vm = VMId_t(-1);
    unsigned lowest_task_count = UINT_MAX;
    
    // For high-priority tasks, find VM with fewest tasks
    if (sla_type == SLA0 || sla_type == SLA1) {
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t info = VM_GetInfo(vm);
                
                // Check if VM is attached to a machine
                if (info.machine_id == MachineId_t(-1)) continue;
                
                // Check if machine is active
                if (activeMachines.find(info.machine_id) == activeMachines.end()) continue;
                
                // Check CPU type and VM type
                if (info.cpu == required_cpu && info.vm_type == required_vm) {
                    // Check GPU requirement
                    MachineInfo_t machineInfo = Machine_GetInfo(info.machine_id);
                    if ((gpu_required && machineInfo.gpus) || !gpu_required) {
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
                }
            } catch (...) {
                continue;
            }
        }
    } 
    // For other tasks, find any suitable VM
    else {
        for (VMId_t vm : vms) {
            if (IsVMMigrating(vm)) continue;
            
            try {
                VMInfo_t info = VM_GetInfo(vm);
                
                // Check if VM is attached to a machine
                if (info.machine_id == MachineId_t(-1)) continue;
                
                // Check if machine is active
                if (activeMachines.find(info.machine_id) == activeMachines.end()) continue;
                
                // Check CPU type and VM type
                if (info.cpu == required_cpu && info.vm_type == required_vm) {
                    // Check GPU requirement
                    MachineInfo_t machineInfo = Machine_GetInfo(info.machine_id);
                    if ((gpu_required && machineInfo.gpus) || !gpu_required) {
                        if (info.active_tasks.size() < lowest_task_count) {
                            lowest_task_count = info.active_tasks.size();
                            target_vm = vm;
                        }
                    }
                }
            } catch (...) {
                continue;
            }
        }
    }
    
    // If no suitable VM found, create a new one
    if (target_vm == VMId_t(-1)) {
        // Find a suitable machine
        MachineId_t target_machine = MachineId_t(-1);
        
        // First check active machines
        for (MachineId_t machine : activeMachines) {
            try {
                MachineInfo_t info = Machine_GetInfo(machine);
                
                // Check CPU type and GPU requirement
                if (info.cpu == required_cpu && ((gpu_required && info.gpus) || !gpu_required)) {
                    target_machine = machine;
                    break;
                }
            } catch (...) {
                continue;
            }
        }
        
        // If no suitable active machine, power on an inactive one
        if (target_machine == MachineId_t(-1)) {
            for (MachineId_t machine : machines) {
                if (activeMachines.find(machine) != activeMachines.end()) continue;
                
                try {
                    MachineInfo_t info = Machine_GetInfo(machine);
                    
                    // Check CPU type and GPU requirement
                    if (info.cpu == required_cpu && ((gpu_required && info.gpus) || !gpu_required)) {
                        Machine_SetState(machine, S0);
                        activeMachines.insert(machine);
                        machineUtilization[machine] = 0.0;
                        target_machine = machine;
                        break;
                    }
                } catch (...) {
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
                
                // For high-priority tasks, set machine to maximum performance
                if (sla_type == SLA0 || sla_type == SLA1) {
                    MachineInfo_t machineInfo = Machine_GetInfo(target_machine);
                    for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                        Machine_SetCorePerformance(target_machine, i, P0);
                    }
                }
            } catch (...) {
                // VM creation failed
            }
        }
    }
    
    // Assign task to VM
    if (target_vm != VMId_t(-1)) {
        // Double-check VM state before adding task
        if (IsVMMigrating(target_vm)) return;
        
        try {
            VMInfo_t info = VM_GetInfo(target_vm);
            
            // Check if VM is attached to a machine
            if (info.machine_id == MachineId_t(-1)) return;
            
            // Check if machine is active
            if (activeMachines.find(info.machine_id) == activeMachines.end()) return;
            
            // Add task to VM
            VM_AddTask(target_vm, task_id, priority);
            
            // For high-priority tasks, ensure machine is at maximum performance
            if (sla_type == SLA0 || sla_type == SLA1) {
                MachineId_t machine = info.machine_id;
                MachineInfo_t machineInfo = Machine_GetInfo(machine);
                for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P0);
                }
            }
        } catch (...) {
            // Task assignment failed
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
            continue;
        }
    }
    
    // Adjust CPU performance based on load and SLA
    for (MachineId_t machine : activeMachines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            
            // Check for high-priority tasks on this machine
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
                            continue;
                        }
                    }
                    
                    // Migrate the VM if found
                    if (vmToMigrate != VMId_t(-1)) {
                        try {
                            VMInfo_t vmInfo = VM_GetInfo(vmToMigrate);
                            CPUType_t vmCpuType = vmInfo.cpu;
                            VMType_t vmType = vmInfo.vm_type;
                            bool needsGPU = false;
                            
                            // Check if any tasks in this VM need GPU
                            for (TaskId_t task : vmInfo.active_tasks) {
                                if (IsTaskGPUCapable(task)) {
                                    needsGPU = true;
                                    break;
                                }
                            }
                            
                            // Find a suitable target machine
                            MachineId_t targetMachine = MachineId_t(-1);
                            double bestUtilization = 0.0;
                            
                            for (MachineId_t machine : activeMachines) {
                                if (machine == sourceMachine) continue;
                                
                                try {
                                    MachineInfo_t machineInfo = Machine_GetInfo(machine);
                                    
                                    // Check CPU type and GPU requirement
                                    if (machineInfo.cpu == vmCpuType && 
                                        ((needsGPU && machineInfo.gpus) || !needsGPU) &&
                                        machineUtilization[machine] > bestUtilization && 
                                        machineUtilization[machine] < OVERLOAD_THRESHOLD) {
                                        
                                        targetMachine = machine;
                                        bestUtilization = machineUtilization[machine];
                                    }
                                } catch (...) {
                                    continue;
                                }
                            }
                            
                            // Perform the migration
                            if (targetMachine != MachineId_t(-1)) {
                                MarkVMAsMigrating(vmToMigrate);
                                VM_Migrate(vmToMigrate, targetMachine);
                            }
                        } catch (...) {
                            // Migration failed, reset state
                            MarkVMAsReady(vmToMigrate);
                        }
                    }
                }
            } catch (...) {
                // Error accessing source machine
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
            continue;
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    for(auto & vm: vms) {
        try {
            VM_Shutdown(vm);
        } catch (...) {
            // Ignore shutdown errors
        }
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // This method is called by MigrationDone
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
                VMType_t vmType = vmInfo.vm_type;
                
                // Check for high-priority tasks
                bool hasHighPriorityTasks = false;
                bool needsGPU = false;
                
                for (TaskId_t task : vmInfo.active_tasks) {
                    SLAType_t slaType = RequiredSLA(task);
                    if (slaType == SLA0 || slaType == SLA1) {
                        hasHighPriorityTasks = true;
                    }
                    
                    if (IsTaskGPUCapable(task)) {
                        needsGPU = true;
                    }
                }
                
                // Find a suitable target machine
                MachineId_t targetMachine = MachineId_t(-1);
                
                for (MachineId_t machine : scheduler.GetMachines()) {
                    if (machine == machine_id) continue;
                    if (!scheduler.IsMachineActive(machine)) continue;
                    
                    try {
                        MachineInfo_t info = Machine_GetInfo(machine);
                        
                        // Check CPU type, VM type, and GPU requirement
                        if (info.cpu == vmCpuType && 
                            ((needsGPU && info.gpus) || !needsGPU)) {
                            
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
                        }
                    } catch (...) {
                        continue;
                    }
                }
                
                // If no suitable active machine, power on a new one
                if (targetMachine == MachineId_t(-1)) {
                    for (MachineId_t machine : scheduler.GetMachines()) {
                        if (scheduler.IsMachineActive(machine)) continue;
                        
                        try {
                            MachineInfo_t info = Machine_GetInfo(machine);
                            
                            // Check CPU type and GPU requirement
                            if (info.cpu == vmCpuType && 
                                ((needsGPU && info.gpus) || !needsGPU)) {
                                
                                Machine_SetState(machine, S0);
                                scheduler.ActivateMachine(machine);
                                targetMachine = machine;
                                break;
                            }
                        } catch (...) {
                            continue;
                        }
                    }
                }
                
                // Perform the migration
                if (targetMachine != MachineId_t(-1)) {
                    scheduler.MarkVMAsMigrating(largestVM);
                    VM_Migrate(largestVM, targetMachine);
                }
            } catch (...) {
                // Error during migration
                scheduler.MarkVMAsReady(largestVM);
            }
        }
        
        // Set all cores to maximum performance to help process tasks faster
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(machine_id, i, P0);
        }
    } catch (...) {
        // Error accessing machine info
    }
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    scheduler.MarkVMAsReady(vm_id);
    scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    static unsigned checkCount = 0;
    checkCount++;
    
    scheduler.PeriodicCheck(time);
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
                    continue;
                }
            }
            
            // Create a VM if needed - match the VM type to the CPU type
            if (!hasVM) {
                try {
                    VMType_t vmType = LINUX; // Default
                    
                    // Use AIX for POWER CPUs
                    if (machineInfo.cpu == POWER) {
                        vmType = AIX;
                    }
                    
                    VMId_t newVM = VM_Create(vmType, machineInfo.cpu);
                    VM_Attach(newVM, machine_id);
                    scheduler.AddVM(newVM);
                } catch (...) {
                    // VM creation failed
                }
            }
        }
        else if (currentState == S5) {
            scheduler.DeactivateMachine(machine_id);
        }
    } catch (...) {
        // Error accessing machine info
    }
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
            
            // Check if VM is attached to a machine
            if (vmInfo.machine_id == MachineId_t(-1)) continue;
            
            // Check if machine is active
            if (!scheduler.IsMachineActive(vmInfo.machine_id)) continue;
            
            for (TaskId_t task : vmInfo.active_tasks) {
                if (task == task_id) {
                    taskVM = vm;
                    taskMachine = vmInfo.machine_id;
                    break;
                }
            }
            
            if (taskVM != VMId_t(-1)) break;
        } catch (...) {
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
                
                // Only attempt to move tasks if VM is not migrating
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
                                
                                // Check if VM is attached to a machine
                                if (otherVMInfo.machine_id == MachineId_t(-1)) continue;
                                
                                // Check if machine is active
                                if (!scheduler.IsMachineActive(otherVMInfo.machine_id)) continue;
                                
                                // Check CPU and VM type compatibility
                                if (otherVMInfo.cpu == vmInfo.cpu && 
                                    otherVMInfo.vm_type == vmInfo.vm_type && 
                                    otherVMInfo.active_tasks.size() < 3) {
                                    
                                    // Check GPU requirement if needed
                                    bool needsGPU = IsTaskGPUCapable(otherTask);
                                    MachineInfo_t otherMachineInfo = Machine_GetInfo(otherVMInfo.machine_id);
                                    
                                    if ((needsGPU && otherMachineInfo.gpus) || !needsGPU) {
                                        targetVM = vm;
                                        break;
                                    }
                                }
                            } catch (...) {
                                continue;
                            }
                        }
                        
                        // Move the task if a suitable VM was found
                        if (targetVM != VMId_t(-1)) {
                            // Use SafeRemoveTask to ensure all safety checks
                            if (scheduler.SafeRemoveTask(taskVM, otherTask)) {
                                try {
                                    VM_AddTask(targetVM, otherTask, MID_PRIORITY);
                                } catch (...) {
                                    // Task addition failed
                                }
                            }
                        }
                    }
                }
            } catch (...) {
                // Error during task movement
            }
        }
        // For SLA2, just increase priority
        else if (slaType == SLA2) {
            SetTaskPriority(task_id, MID_PRIORITY);
        }
    }
}