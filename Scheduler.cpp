//
//  Scheduler.cpp
//  CloudSim
//
#include "Scheduler.hpp"
#include <iostream>
#include "Internal_Interfaces.h"

// Static variables
static Scheduler scheduler;

bool Scheduler::HasActiveJobs(MachineId_t machineId) {
    try {
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machineId && !vmInfo.active_tasks.empty()) {
                return true;
            }
        }
        
        return false;
    } catch (...) {
        return true;
    }
}

bool Scheduler::EnsureMachineAwake(MachineId_t machineId) {
    try {
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        if (info.s_state != S0) {
            SimOutput("EnsureMachineAwake(): Waking up machine " + std::to_string(machineId), 3);
            Machine_SetState(machineId, S0);
            activeMachines.insert(machineId);
            return true;
        }
        return true;
    } catch (...) {
        SimOutput("ERROR: Failed to wake up machine " + std::to_string(machineId), 1);
        return false;
    }
}

bool Scheduler::IsCompatibleVMCPU(VMType_t vmType, CPUType_t cpuType) {
    if (vmType == LINUX || vmType == LINUX_RT) {
        return true;
    } else if (vmType == WIN) {
        return (cpuType == ARM || cpuType == X86);
    } else if (vmType == AIX) {
        return (cpuType == POWER);
    }
    return false;
}

TaskClass_t Scheduler::GetTaskClass(TaskId_t taskId) {
    try {
        // Get task information
        uint64_t instructions = GetRemainingInstructions(taskId);
        unsigned memory = GetTaskMemory(taskId);
        bool isGPUCapable = IsTaskGPUCapable(taskId);
        
        // Define thresholds for classification
        const uint64_t SHORT_TASK = 1000000000;        // 1 billion instructions
        const uint64_t MEDIUM_TASK = 3000000000;       // 3 billion instructions
        const uint64_t LONG_TASK = 5000000000;         // 5 billion instructions
        const unsigned HIGH_MEMORY = 16;               // 16 GB memory requirement
        
        // Apply heuristics for classification
        if (isGPUCapable) {
            if (instructions > LONG_TASK) {
                return SCIENTIFIC;  // HPC: Long, compute-intensive, GPU-enabled
            } else if (instructions < SHORT_TASK) {
                return CRYPTO;      // Short, compute-intensive, GPU-enabled
            } else {
                return AI_TRAINING;  // Medium, compute-intensive, GPU-enabled
            }
        } else {
            if (instructions < SHORT_TASK) {
                return WEB_REQUEST;  // Short requests
            } else if (memory > HIGH_MEMORY) {
                return STREAMING;    // Streaming: medium memory requirements
            } else {
                // Default to web for medium tasks without special requirements
                return WEB_REQUEST;
            }
        }
    } catch (...) {
        // Fallback to simple classification based on task ID if we can't get task info
        if (taskId % 5 == 0) return AI_TRAINING;
        if (taskId % 5 == 1) return CRYPTO;
        if (taskId % 5 == 2) return SCIENTIFIC;
        if (taskId % 5 == 3) return STREAMING;
        return WEB_REQUEST;
    }
}

void Scheduler::CreateVMsForAllCPUTypes() {
    for (const auto& entry : cpuTypeMachines) {
        CPUType_t cpuType = entry.first;
        const std::vector<MachineId_t>& machinesOfType = entry.second;
        
        if (machinesOfType.empty()) continue;
        
        MachineId_t machineId = machinesOfType[0];
        
        if (activeMachines.find(machineId) == activeMachines.end()) {
            Machine_SetState(machineId, S0);
            activeMachines.insert(machineId);
        }
        
        try {
            VMId_t linuxVM = VM_Create(LINUX, cpuType);
            vms.push_back(linuxVM);
            VM_Attach(linuxVM, machineId);
            vmToMachine[linuxVM] = machineId;
            SimOutput("Created Linux VM for CPU type " + std::to_string(cpuType), 2);
            
            for (TaskClass_t taskClass : {AI_TRAINING, CRYPTO, SCIENTIFIC, STREAMING, WEB_REQUEST}) {
                taskClassToVMs[taskClass].push_back(linuxVM);
            }
        } catch (const std::exception& e) {
            SimOutput("ERROR: Failed to create Linux VM for CPU " + 
                     std::to_string(cpuType) + ": " + e.what(), 1);
        }
        
        try {
            VMId_t linuxRTVM = VM_Create(LINUX_RT, cpuType);
            vms.push_back(linuxRTVM);
            VM_Attach(linuxRTVM, machineId);
            vmToMachine[linuxRTVM] = machineId;
            SimOutput("Created Linux_RT VM for CPU type " + std::to_string(cpuType), 2);
            
            taskClassToVMs[AI_TRAINING].push_back(linuxRTVM);
            taskClassToVMs[SCIENTIFIC].push_back(linuxRTVM);
        } catch (const std::exception& e) {
            SimOutput("ERROR: Failed to create Linux_RT VM for CPU " + 
                     std::to_string(cpuType) + ": " + e.what(), 1);
        }
        
        if (cpuType == ARM || cpuType == X86) {
            try {
                VMId_t winVM = VM_Create(WIN, cpuType);
                vms.push_back(winVM);
                VM_Attach(winVM, machineId);
                vmToMachine[winVM] = machineId;
                SimOutput("Created Windows VM for CPU type " + std::to_string(cpuType), 2);
                
                taskClassToVMs[STREAMING].push_back(winVM);
                taskClassToVMs[WEB_REQUEST].push_back(winVM);
            } catch (const std::exception& e) {
                SimOutput("ERROR: Failed to create Windows VM for CPU " + 
                         std::to_string(cpuType) + ": " + e.what(), 1);
            }
        }
        
        if (cpuType == POWER) {
            try {
                VMId_t aixVM = VM_Create(AIX, cpuType);
                vms.push_back(aixVM);
                VM_Attach(aixVM, machineId);
                vmToMachine[aixVM] = machineId;
                SimOutput("Created AIX VM for CPU type " + std::to_string(cpuType), 2);
                
                taskClassToVMs[SCIENTIFIC].push_back(aixVM);
            } catch (const std::exception& e) {
                SimOutput("ERROR: Failed to create AIX VM for CPU " + 
                         std::to_string(cpuType) + ": " + e.what(), 1);
            }
        }
    }
}

std::vector<VMId_t> Scheduler::GetCompatibleVMs(CPUType_t cpuType, VMType_t vmType) {
    std::vector<VMId_t> compatibleVMs;
    
    for (VMId_t vm : vms) {
        if (VM_IsPendingMigration(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        if (vmInfo.vm_type == vmType && vmInfo.cpu == cpuType) {
            compatibleVMs.push_back(vm);
        }
    }
    
    return compatibleVMs;
}

void Scheduler::UpdateMachineUtilization() {
    for (MachineId_t machineId : machines) {
        if (activeMachines.find(machineId) != activeMachines.end()) {
            machineUtilization[machineId] = CalculateMachineUtilization(machineId);
        }
    }
}

double Scheduler::CalculateMachineUtilization(MachineId_t machineId) {
    MachineInfo_t info = Machine_GetInfo(machineId);
    unsigned totalCores = info.num_cpus;
    unsigned activeCores = 0;
    
    for (VMId_t vm : vms) {
        VMInfo_t vmInfo = VM_GetInfo(vm);
        if (vmInfo.machine_id == machineId) {
            activeCores += vmInfo.active_tasks.size();
        }
    }
    
    return (totalCores > 0) ? static_cast<double>(activeCores) / totalCores : 0.0;
}

MachineId_t Scheduler::FindSuitableMachine(CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded) {
    for (MachineId_t machineId : machines) {
        if (activeMachines.find(machineId) == activeMachines.end()) continue;
        
        MachineInfo_t info = Machine_GetInfo(machineId);
        if (info.cpu != cpuType) continue;
        if (needsGPU && !info.gpus) continue;
        if (info.memory_used + memoryNeeded > info.memory_size) continue;
        
        double utilization = machineUtilization[machineId];
        if (utilization < HIGH_UTIL_THRESHOLD) {
            return machineId;
        }
    }
    return MachineId_t(-1);
}

MachineId_t Scheduler::PowerOnMachine(CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded) {
    for (MachineId_t machineId : machines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            
            if (info.cpu != cpuType) continue;
            if (needsGPU && !info.gpus) continue;
            if (info.memory_size < memoryNeeded) continue;
            
            if (info.s_state != S0) {
                SimOutput("PowerOnMachine(): Waking up machine "  + std::to_string(machineId), 3);
                Machine_SetState(machineId, S0);
            }
            
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P1);
            }
            
            activeMachines.insert(machineId);
            machineUtilization[machineId] = 0.0;
            return machineId;
        } catch (...) {
            continue;
        }
    }
    return MachineId_t(-1);
}

MachineId_t Scheduler::FindMigrationTarget(VMId_t vm, MachineId_t sourceMachine) {
    VMInfo_t vmInfo = VM_GetInfo(vm);
    
    unsigned memoryNeeded = 0;
    bool needsGPU = false;
    
    for (TaskId_t taskId : vmInfo.active_tasks) {
        TaskInfo_t tinfo = GetTaskInfo(taskId);
        memoryNeeded += tinfo.required_memory;
        if (tinfo.gpu_capable) {
            needsGPU = true;
        }
    }
    
    memoryNeeded += 8;
    
    for (MachineId_t machineId : machines) {
        if (machineId == sourceMachine) continue;
        
        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            
            if (info.s_state != S0) {
                if (info.s_state >= S3) continue;
                
                Machine_SetState(machineId, S0);
                activeMachines.insert(machineId);
            }
            
            if (info.cpu != Machine_GetInfo(sourceMachine).cpu) continue;
            if (needsGPU && !info.gpus) continue;
            if (info.memory_used + memoryNeeded > info.memory_size) continue;
            
            double utilization = machineUtilization[machineId];
            if (utilization < HIGH_UTIL_THRESHOLD) {
                return machineId;
            }
        } catch (...) {
            continue;
        }
    }
    
    MachineId_t newMachine = PowerOnMachine(Machine_GetInfo(sourceMachine).cpu, needsGPU, memoryNeeded);
    
    if (newMachine != MachineId_t(-1)) {
        try {
            MachineInfo_t info = Machine_GetInfo(newMachine);
            if (info.s_state != S0) {
                Machine_SetState(newMachine, S0);
            }
        } catch (...) {
            return MachineId_t(-1);
        }
    }
    
    return newMachine;
}

void Scheduler::MigrateVMFromOverloadedMachine(MachineId_t machineId) {
    std::vector<VMId_t> machineVMs;
    for (VMId_t vm : vms) {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.machine_id == machineId && !VM_IsPendingMigration(vm)) {
            machineVMs.push_back(vm);
        }
    }
    
    std::sort(machineVMs.begin(), machineVMs.end(), 
        [this](VMId_t a, VMId_t b) {
            return VM_GetInfo(a).active_tasks.size() < VM_GetInfo(b).active_tasks.size();
        });
    
    for (VMId_t vm : machineVMs) {
        MachineId_t targetMachine = FindMigrationTarget(vm, machineId);
        if (targetMachine != MachineId_t(-1)) {
            if (EnsureMachineAwake(targetMachine)) {
                try {
                    VM_MigrationStarted(vm);
                    Machine_MigrateVM(vm, machineId, targetMachine);
                    vmToMachine[vm] = targetMachine;
                    return;
                } catch (const std::exception& e) {
                    SimOutput("ERROR: Migration failed: " + std::string(e.what()), 1);
                }
            }
        }
    }
}

void Scheduler::AdjustMachinePowerState(MachineId_t machineId, double utilization) {
    if (pendingStateChanges[machineId]) return;
    
    MachineInfo_t info = Machine_GetInfo(machineId);
    
    // Check if machine has any VMs
    bool hasVMs = false;
    bool hasActiveJobs = false;
    bool hasHighPriorityTasks = false;
    bool hasComputeIntensiveTasks = false;
    
    for (VMId_t vm : vms) {
        VMInfo_t vmInfo = VM_GetInfo(vm);
        if (vmInfo.machine_id != machineId) continue;
        
        hasVMs = true;
        
        if (!vmInfo.active_tasks.empty()) {
            hasActiveJobs = true;
            
            for (TaskId_t task : vmInfo.active_tasks) {
                SLAType_t sla = RequiredSLA(task);
                TaskClass_t taskClass = GetTaskClass(task);
                
                if (sla == SLA0 || sla == SLA1) {
                    hasHighPriorityTasks = true;
                }
                
                if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC || taskClass == CRYPTO) {
                    hasComputeIntensiveTasks = true;
                }
                
                if (hasHighPriorityTasks && hasComputeIntensiveTasks) break;
            }
        }
        
        if (hasHighPriorityTasks && hasComputeIntensiveTasks) break;
    }
    
    if (!hasActiveJobs) {
        // Has VMs but no active jobs, can use deeper sleep states
        for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P2);
            }
    } else {
        // Has active jobs, adjust based on workload characteristics
        if (hasHighPriorityTasks) {
            // High-priority tasks need maximum performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P0);
            }
        } else if (hasComputeIntensiveTasks) {
            // Compute-intensive tasks need good performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P1);
            }
        } else if (utilization < LOW_UTIL_THRESHOLD) {
            // Low utilization, can use lower performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P3);
            }
        } else if (utilization < 0.5) {
            // Moderate utilization
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P2);
            }
        } else {
            // Higher utilization
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P1);
            }
        }
    }
}

// Main scheduler functions
void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing workload-aware scheduler", 1);
    
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        cpuTypeMachines[info.cpu].push_back(machineId);
        
        Machine_SetState(machineId, S0);
        activeMachines.insert(machineId);
        
        for (unsigned j = 0; j < info.num_cpus; j++) {
            Machine_SetCorePerformance(machineId, j, P2);
        }
    }
    
    CreateVMsForAllCPUTypes();
    
    for (unsigned i = 16; i < machines.size(); i++) {
        MachineId_t machineId = machines[i];
        bool hasVMs = false;
        
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machineId) {
                hasVMs = true;
                break;
            }
        }
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task requirements
    CPUType_t requiredCPU = RequiredCPUType(task_id);
    VMType_t requiredVM = RequiredVMType(task_id);
    SLAType_t slaType = RequiredSLA(task_id);
    bool needsGPU = IsTaskGPUCapable(task_id);
    unsigned memoryNeeded = GetTaskMemory(task_id);
    TaskClass_t taskClass = GetTaskClass(task_id);
    
    // Track high-priority tasks
    if (slaType == SLA0 || slaType == SLA1) {
        highPriorityTasks.insert(task_id);
    }
    
    // Ensure VM-CPU compatibility
    if (!IsCompatibleVMCPU(requiredVM, requiredCPU)) {
        if (requiredCPU == POWER) {
            requiredVM = AIX;
        } else if (requiredCPU == ARM || requiredCPU == X86) {
            if (requiredVM == AIX) {
                requiredVM = LINUX;
            }
        } else {
            requiredVM = LINUX;
        }
    }
    
    // Determine priority
    Priority_t priority;
    switch (slaType) {
        case SLA0: priority = HIGH_PRIORITY; break;
        case SLA1: priority = HIGH_PRIORITY; break;
        case SLA2: priority = MID_PRIORITY; break;
        default: priority = LOW_PRIORITY;
    }
    
    // For high-priority tasks, try to find optimal placement
    VMId_t targetVM = VMId_t(-1);
    
    if (slaType == SLA0 || slaType == SLA1) {
        // Find machine with lowest utilization that meets requirements
        MachineId_t bestMachine = MachineId_t(-1);
        double lowestUtil = 1.0;
        
        for (MachineId_t machineId : machines) {
            if (activeMachines.find(machineId) == activeMachines.end()) continue;
            
            MachineInfo_t info = Machine_GetInfo(machineId);
            if (info.cpu != requiredCPU) continue;
            if (needsGPU && !info.gpus) continue;
            if (info.memory_used + memoryNeeded > info.memory_size) continue;
            
            double util = machineUtilization[machineId];
            if (util < lowestUtil) {
                lowestUtil = util;
                bestMachine = machineId;
            }
        }
        
        if (bestMachine != MachineId_t(-1)) {
            // Find or create VM on this machine
            for (VMId_t vm : vms) {
                if (VM_IsPendingMigration(vm)) continue;
                
                VMInfo_t vmInfo = VM_GetInfo(vm);
                if (vmInfo.machine_id != bestMachine) continue;
                if (vmInfo.vm_type != requiredVM) continue;
                if (vmInfo.cpu != requiredCPU) continue;
                
                targetVM = vm;
                break;
            }
            
            if (targetVM == VMId_t(-1)) {
                try {
                    targetVM = VM_Create(requiredVM, requiredCPU);
                    vms.push_back(targetVM);
                    VM_Attach(targetVM, bestMachine);
                    vmToMachine[targetVM] = bestMachine;
                    taskClassToVMs[taskClass].push_back(targetVM);
                } catch (...) {
                    targetVM = VMId_t(-1);
                }
            }
        }
    }
    
    // If no optimal placement found, use standard placement
    if (targetVM == VMId_t(-1)) {
        targetVM = FindBestVM(task_id, taskClass, requiredCPU, requiredVM, needsGPU, memoryNeeded);
        
        if (targetVM == VMId_t(-1)) {
            targetVM = CreateNewVM(requiredVM, requiredCPU, needsGPU, memoryNeeded);
        }
    }
    
    // Assign task to VM
    if (targetVM != VMId_t(-1)) {
        try {
            VM_AddTask(targetVM, task_id, priority);
            
            // Set appropriate performance level based on task class and SLA
            MachineId_t machineId = vmToMachine[targetVM];
            MachineInfo_t info = Machine_GetInfo(machineId);
            
            for (unsigned i = 0; i < info.num_cpus; i++) {
                if (slaType == SLA0 || slaType == SLA1) {
                    // High-priority tasks always get maximum performance
                    Machine_SetCorePerformance(machineId, i, P0);
                } else if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC) {
                    // Compute-intensive tasks get good performance
                    Machine_SetCorePerformance(machineId, i, P1);
                } else if (taskClass == CRYPTO || taskClass == STREAMING) {
                    // Medium tasks get moderate performance
                    Machine_SetCorePerformance(machineId, i, P2);
                } else {
                    // Web tasks can use lower performance
                    Machine_SetCorePerformance(machineId, i, P3);
                }
            }
        } catch (...) {
            // For high-priority tasks that couldn't be scheduled, add to waiting list
            if (slaType == SLA0 || slaType == SLA1) {
                highPriorityTasks.insert(task_id);
            }
        }
    } else if (slaType == SLA0 || slaType == SLA1) {
        // Add high-priority tasks to waiting list if no VM was found
        highPriorityTasks.insert(task_id);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    UpdateMachineUtilization();
    
    for (auto& pair : machineUtilization) {
        MachineId_t machineId = pair.first;
        double utilization = pair.second;
        
        if (utilization > HIGH_UTIL_THRESHOLD) {
            MigrateVMFromOverloadedMachine(machineId);
        } else if (utilization < LOW_UTIL_THRESHOLD && now - lastConsolidationTime > CONSOLIDATION_INTERVAL) {
            AdjustMachinePowerState(machineId, utilization);
        }
    }
    
    if (now - lastConsolidationTime > CONSOLIDATION_INTERVAL) {
        ConsolidateVMs(now);
        lastConsolidationTime = now;
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Clean up task tracking
    slaViolatedTasks.erase(task_id);
    highPriorityTasks.erase(task_id);
    
    // Update machine utilization and adjust power states
    UpdateMachineUtilization();
    
    // Check if any machines can be adjusted for power efficiency
    for (MachineId_t machineId : machines) {
        if (activeMachines.find(machineId) == activeMachines.end()) continue;
        
        double utilization = machineUtilization[machineId];
        
        // Check if machine has any high-priority tasks
        bool hasHighPriorityTasks = false;
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machineId) continue;
            
            for (TaskId_t t : vmInfo.active_tasks) {
                SLAType_t sla = RequiredSLA(t);
                if (sla == SLA0 || sla == SLA1) {
                    hasHighPriorityTasks = true;
                    break;
                }
            }
            if (hasHighPriorityTasks) break;
        }
        
        // If no high-priority tasks and low utilization, adjust power
        if (!hasHighPriorityTasks && utilization < LOW_UTIL_THRESHOLD) {
            AdjustMachinePowerState(machineId, utilization);
        }
    }
    
    // Periodically consolidate VMs
    if (now - lastConsolidationTime > CONSOLIDATION_INTERVAL) {
        ConsolidateVMs(now);
        lastConsolidationTime = now;
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // VM migration is complete, update state
    VM_MigrationCompleted(vm_id);
    
    // Update machine utilization after migration
    VMInfo_t vmInfo = VM_GetInfo(vm_id);
    MachineId_t machineId = vmInfo.machine_id;
    machineUtilization[machineId] = CalculateMachineUtilization(machineId);
}

void Scheduler::Shutdown(Time_t time) {
    double totalEnergy = Machine_GetClusterEnergy();
    SimOutput("SimulationComplete(): Total energy consumed: " + std::to_string(totalEnergy), 1);
    
    for (VMId_t vm : vms) {
        VM_Shutdown(vm);
    }
    
    for (MachineId_t machine : machines) {
        Machine_SetState(machine, S5);
    }
    
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + std::to_string(time), 4);
}

VMId_t Scheduler::FindBestVM(TaskId_t task_id, TaskClass_t taskClass, CPUType_t requiredCPU, 
                            VMType_t requiredVM, bool needsGPU, unsigned memoryNeeded) {
    for (VMId_t vm : taskClassToVMs[taskClass]) {
        if (VM_IsPendingMigration(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        if (vmInfo.vm_type != requiredVM) continue;
        if (vmInfo.cpu != requiredCPU) continue;
        
        MachineId_t machineId = vmInfo.machine_id;
        MachineInfo_t machineInfo = Machine_GetInfo(machineId);
        
        if (machineInfo.cpu != requiredCPU) continue;
        if (needsGPU && !machineInfo.gpus) continue;
        if (machineInfo.memory_used + memoryNeeded > machineInfo.memory_size) continue;
        
        double utilization = machineUtilization[machineId];
        if (utilization < HIGH_UTIL_THRESHOLD) {
            return vm;
        }
    }
    
    for (VMId_t vm : vms) {
        if (VM_IsPendingMigration(vm)) continue;
        
        VMInfo_t vmInfo = VM_GetInfo(vm);
        if (vmInfo.vm_type != requiredVM) continue;
        if (vmInfo.cpu != requiredCPU) continue;
        
        MachineId_t machineId = vmInfo.machine_id;
        MachineInfo_t machineInfo = Machine_GetInfo(machineId);
        
        if (machineInfo.cpu != requiredCPU) continue;
        if (needsGPU && !machineInfo.gpus) continue;
        if (machineInfo.memory_used + memoryNeeded > machineInfo.memory_size) continue;
        
        return vm;
    }
    
    return VMId_t(-1);
}

void Scheduler::HandleSLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Task " + std::to_string(task_id) + " is at risk of SLA violation", 2);
    
    // Mark this task as having an SLA violation
    slaViolatedTasks[task_id] = true;
    
    // Find which VM is running this task
    VMId_t targetVM = VMId_t(-1);
    MachineId_t machineId = MachineId_t(-1);
    
    for (VMId_t vm : vms) {
        VMInfo_t vmInfo = VM_GetInfo(vm);
        for (TaskId_t t : vmInfo.active_tasks) {
            if (t == task_id) {
                targetVM = vm;
                machineId = vmInfo.machine_id;
                break;
            }
        }
        if (targetVM != VMId_t(-1)) break;
    }
    
    if (targetVM == VMId_t(-1)) return;
    
    // Get task characteristics
    SLAType_t slaType = RequiredSLA(task_id);
    TaskClass_t taskClass = GetTaskClass(task_id);
    
    // Take action based on SLA type and task class
    if (slaType == SLA0 || slaType == SLA1) {
        // For high-priority SLAs, take immediate action
        SetTaskPriority(task_id, HIGH_PRIORITY);
        
        // Set machine to maximum performance
        MachineInfo_t info = Machine_GetInfo(machineId);
        for (unsigned i = 0; i < info.num_cpus; i++) {
            Machine_SetCorePerformance(machineId, i, P0);
        }
        
        // For compute-intensive tasks, consider migration to a better machine
        if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC || taskClass == CRYPTO) {
            double utilization = machineUtilization[machineId];
            if (utilization > 0.7) {
                MachineId_t targetMachine = FindMigrationTarget(targetVM, machineId);
                if (targetMachine != MachineId_t(-1)) {
                    try {
                        VM_MigrationStarted(targetVM);
                        Machine_MigrateVM(targetVM, machineId, targetMachine);
                        vmToMachine[targetVM] = targetMachine;
                        SimOutput("Migrating VM with at-risk task to machine " + 
                                     std::to_string(targetMachine), 2);
                    }
                    catch (...) {
                    }
                }
            }
        }
    } else if (slaType == SLA2) {
        // For medium-priority SLAs
        if (GetTaskPriority(task_id) == LOW_PRIORITY) {
            SetTaskPriority(task_id, MID_PRIORITY);
        }
        
        // Adjust performance based on task class
        MachineInfo_t info = Machine_GetInfo(machineId);
        if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC) {
            // Compute-intensive tasks need higher performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P0);
            }
        } else {
            // Other tasks can use moderate performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machineId, i, P1);
            }
        }
    }
}

// Optimized StateChangeComplete handler
void Scheduler::HandleStateChangeComplete(Time_t time, MachineId_t machine_id) {
    pendingStateChanges[machine_id] = false;
    
    MachineInfo_t info = Machine_GetInfo(machine_id);
    if (info.s_state == S0) {
        activeMachines.insert(machine_id);
        machineUtilization[machine_id] = CalculateMachineUtilization(machine_id);
        
        // Process any waiting high-priority tasks
        for (auto it = highPriorityTasks.begin(); it != highPriorityTasks.end(); ) {
            TaskId_t task_id = *it;
            
            CPUType_t requiredCPU = RequiredCPUType(task_id);
            VMType_t requiredVM = RequiredVMType(task_id);
            bool needsGPU = IsTaskGPUCapable(task_id);
            unsigned memoryNeeded = GetTaskMemory(task_id);
            TaskClass_t taskClass = GetTaskClass(task_id);
            
            // Check if this machine is suitable
            if (info.cpu == requiredCPU && 
                (!needsGPU || info.gpus) && 
                info.memory_used + memoryNeeded <= info.memory_size) {
                
                // Find or create a suitable VM
                VMId_t targetVM = FindBestVM(task_id, taskClass, requiredCPU, 
                                           requiredVM, needsGPU, memoryNeeded);
                
                if (targetVM == VMId_t(-1)) {
                    targetVM = CreateNewVM(requiredVM, requiredCPU, needsGPU, memoryNeeded);
                }
                
                if (targetVM != VMId_t(-1)) {
                    try {
                        VM_AddTask(targetVM, task_id, HIGH_PRIORITY);
                        
                        // Set appropriate performance level based on task class
                        for (unsigned i = 0; i < info.num_cpus; i++) {
                            if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC) {
                                Machine_SetCorePerformance(machine_id, i, P0);
                            } else if (taskClass == CRYPTO || taskClass == STREAMING) {
                                Machine_SetCorePerformance(machine_id, i, P1);
                            } else {
                                Machine_SetCorePerformance(machine_id, i, P2);
                            }
                        }
                        
                        it = highPriorityTasks.erase(it);
                        continue;
                    } catch (...) {}
                }
            }
            ++it;
        }
    } else if (info.s_state == S5) {
        activeMachines.erase(machine_id);
    }
}

bool Scheduler::RequestMachineStateChange(MachineId_t machineId, MachineState_t newState) {
    try {
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        // If already in requested state, no need to change
        if (info.s_state == newState) {
            return true;
        }
        
        // Check if we can change state
        if (newState != S0 && HasActiveJobs(machineId)) {
            SimOutput("Cannot change machine " + std::to_string(machineId) + 
                     " to state " + std::to_string(newState) + " - has active jobs", 2);
            return false;
        }
        
        // Request state change
        Machine_SetState(machineId, newState);
        pendingStateChanges[machineId] = true;
        
        // Update active machines tracking
        if (newState == S0) {
            activeMachines.insert(machineId);
        } else if (newState == S5) {
            activeMachines.erase(machineId);
        }
        
        return true;
    } catch (const std::exception& e) {
        SimOutput("ERROR in RequestMachineStateChange: " + std::string(e.what()), 1);
        return false;
    }
}

VMId_t Scheduler::CreateNewVM(VMType_t vmType, CPUType_t cpuType, bool needsGPU, unsigned memoryNeeded) {
    if (!IsCompatibleVMCPU(vmType, cpuType)) {
        SimOutput("WARNING: Incompatible VM-CPU combination: VM=" + std::to_string(vmType) + 
                 ", CPU=" + std::to_string(cpuType), 1);
        
        if (vmType == WIN) {
            cpuType = X86;
        } else if (vmType == AIX) {
            cpuType = POWER;
        } else {
            cpuType = X86;
        }
    }
    
    MachineId_t targetMachine = FindSuitableMachine(cpuType, needsGPU, memoryNeeded);
    
    if (targetMachine == MachineId_t(-1)) {
        targetMachine = PowerOnMachine(cpuType, needsGPU, memoryNeeded);
    }
    
    if (targetMachine != MachineId_t(-1)) {
        MachineInfo_t machineInfo = Machine_GetInfo(targetMachine);
        if (!IsCompatibleVMCPU(vmType, machineInfo.cpu)) {
            SimOutput("ERROR: Cannot create VM type " + std::to_string(vmType) + 
                     " on CPU type " + std::to_string(machineInfo.cpu), 1);
            return VMId_t(-1);
        }
        
        VMId_t newVM;
        try {
            newVM = VM_Create(vmType, machineInfo.cpu);
            VM_Attach(newVM, targetMachine);
            vms.push_back(newVM);
            vmToMachine[newVM] = targetMachine;
            
            for (TaskClass_t taskClass : {AI_TRAINING, CRYPTO, SCIENTIFIC, STREAMING, WEB_REQUEST}) {
                if ((taskClass == AI_TRAINING || taskClass == SCIENTIFIC) && machineInfo.gpus) {
                    taskClassToVMs[taskClass].push_back(newVM);
                } else if (taskClass != AI_TRAINING && taskClass != SCIENTIFIC) {
                    taskClassToVMs[taskClass].push_back(newVM);
                }
            }
            
            return newVM;
        } catch (const std::exception& e) {
            SimOutput("ERROR: Failed to create VM: " + std::string(e.what()), 1);
            return VMId_t(-1);
        }
    }
    
    return VMId_t(-1);
}

void Scheduler::ConsolidateVMs(Time_t now) {
    std::vector<MachineId_t> underutilizedMachines;
    for (auto& pair : machineUtilization) {
        if (pair.second < LOW_UTIL_THRESHOLD) {
            underutilizedMachines.push_back(pair.first);
        }
    }
    
    std::sort(underutilizedMachines.begin(), underutilizedMachines.end(), 
         [this](MachineId_t a, MachineId_t b) {
             return machineUtilization[a] < machineUtilization[b];
         });
    
    for (MachineId_t source : underutilizedMachines) {
        if (HasActiveJobs(source)) {
            SimOutput("ConsolidateVMs: Machine " + std::to_string(source) + 
                     " has active jobs, skipping consolidation", 3);
            continue;
        }
        
        std::vector<VMId_t> machineVMs;
        for (VMId_t vm : vms) {
            VMInfo_t info = VM_GetInfo(vm);
            if (info.machine_id == source && !VM_IsPendingMigration(vm) && !VM_IsPendingMigration(vm)) {
                machineVMs.push_back(vm);
            }
        }
        
        // if (machineVMs.empty()) {
        //     SimOutput("ConsolidateVMs: Machine " + std::to_string(source) + 
        //              " has no VMs, powering down", 3);
        //     Machine_SetState(source, S3);
        //     activeMachines.erase(source);
        //     continue;
        // }
        
        bool allMigrated = true;
        for (VMId_t vm : machineVMs) {
            MachineId_t target = FindMigrationTarget(vm, source);
            if (target != MachineId_t(-1)) {
                if (EnsureMachineAwake(target)) {
                    try {
                        VM_MigrationStarted(vm);
                        Machine_MigrateVM(vm, source, target);
                        vmToMachine[vm] = target;
                    } catch (const std::exception& e) {
                        SimOutput("ERROR: Migration failed: " + std::string(e.what()), 1);
                        allMigrated = false;
                    }
                } else {
                    allMigrated = false;
                }
            } else {
                allMigrated = false;
                break;
            }
        }
        
        // if (allMigrated && !machineVMs.empty()) {
        //     if (!HasActiveJobs(source)) {
        //         SimOutput("ConsolidateVMs: All VMs migrated from machine " + 
        //                  std::to_string(source) + ", powering down", 3);
        //         Machine_SetState(source, S3);
        //         activeMachines.erase(source);
        //     } else {
        //         SimOutput("WARNING: Machine " + std::to_string(source) + 
        //                  " still has active jobs after VM migration. Cannot power down.", 2);
        //     }
        // }
    }
}

// Public interface functions
void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + std::to_string(task_id) + " at time " + std::to_string(time), 4);
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + std::to_string(task_id) + " completed at time " + std::to_string(time), 4);
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Machine " + std::to_string(machine_id) + " is running low on memory at time " + std::to_string(time), 2);
    scheduler.MigrateVMFromOverloadedMachine(machine_id);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): VM " + std::to_string(vm_id) + " migration completed at time " + std::to_string(time), 4);
    scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation completed at time " + std::to_string(time), 1);
    scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Task " + std::to_string(task_id) + " at risk of SLA violation at time " + std::to_string(time), 2);
    scheduler.HandleSLAWarning(time, task_id);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + std::to_string(machine_id) + " completed state change at time " + std::to_string(time), 3);
    scheduler.HandleStateChangeComplete(time, machine_id);
}