//
//  Scheduler.cpp
//  CloudSim
//
#include "Scheduler.hpp"
#include <iostream>
#include "Internal_Interfaces.h"

// Static variables
static Scheduler scheduler;

// Task information helper functions
TaskClass_t Scheduler::GetTaskClass(TaskId_t taskId) {
    if (taskId % 5 == 0) return AI_TRAINING;
    if (taskId % 5 == 1) return CRYPTO;
    if (taskId % 5 == 2) return SCIENTIFIC;
    if (taskId % 5 == 3) return STREAMING;
    return WEB_REQUEST;
}

bool Scheduler::IsGPUCapable(TaskId_t taskId) {
    TaskClass_t taskClass = GetTaskClass(taskId);
    return (taskClass == AI_TRAINING || taskClass == SCIENTIFIC);
}

unsigned Scheduler::GetMemory(TaskId_t taskId) {
    try {
        return ::GetTaskMemory(taskId);
    } catch (...) {
        TaskClass_t taskClass = GetTaskClass(taskId);
        switch (taskClass) {
            case AI_TRAINING: return 32;
            case SCIENTIFIC: return 24;
            case STREAMING: return 16;
            case CRYPTO: return 8;
            case WEB_REQUEST: return 4;
            default: return 8;
        }
    }
}

CPUType_t Scheduler::RequiredCPUType(TaskId_t taskId) {
    try {
        return ::RequiredCPUType(taskId);
    } catch (...) {
        TaskClass_t taskClass = GetTaskClass(taskId);
        if (taskClass == AI_TRAINING) return X86;
        if (taskClass == SCIENTIFIC) return X86;
        if (taskClass == CRYPTO) return X86;
        return X86;
    }
}

VMType_t Scheduler::RequiredVMType(TaskId_t taskId) {
    try {
        return ::RequiredVMType(taskId);
    } catch (...) {
        TaskClass_t taskClass = GetTaskClass(taskId);
        CPUType_t cpuType = RequiredCPUType(taskId);
        
        if (cpuType == POWER) {
            return AIX;
        } else if (taskClass == STREAMING && (cpuType == X86 || cpuType == ARM)) {
            return WIN;
        } else if (taskClass == AI_TRAINING || taskClass == SCIENTIFIC) {
            return LINUX_RT;
        }
        
        return LINUX;
    }
}

SLAType_t Scheduler::RequiredSLA(TaskId_t taskId) {
    try {
        return ::RequiredSLA(taskId);
    } catch (...) {
        TaskClass_t taskClass = GetTaskClass(taskId);
        if (taskClass == WEB_REQUEST) return SLA0;
        if (taskClass == STREAMING) return SLA1;
        if (taskClass == AI_TRAINING) return SLA2;
        return SLA3;
    }
}

// VM and Machine Management
bool Scheduler::IsVMMigrating(VMId_t vm) const {
    return migratingVMs.find(vm) != migratingVMs.end();
}

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

bool Scheduler::IsVMPendingMigration(VMId_t vm) const {
    try {
        return VM_IsPendingMigration(vm);
    } catch (...) {
        return false;
    }
}

std::vector<VMId_t> Scheduler::GetCompatibleVMs(CPUType_t cpuType, VMType_t vmType) {
    std::vector<VMId_t> compatibleVMs;
    
    for (VMId_t vm : vms) {
        if (IsVMMigrating(vm)) continue;
        
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

bool Scheduler::PrepareVMForMigration(VMId_t vm) {
    try {
        // Check if VM is already migrating
        if (IsVMMigrating(vm)) {
            SimOutput("VM " + std::to_string(vm) + " is already migrating", 2);
            return false;
        }
        
        // Check if VM has pending migration
        if (VM_IsPendingMigration(vm)) {
            SimOutput("VM " + std::to_string(vm) + " already has pending migration", 2);
            return false;
        }
        
        // Mark VM as pending migration
        VM_MigrationStarted(vm);
        migratingVMs.insert(vm);
        return true;
    } catch (const std::exception& e) {
        SimOutput("ERROR preparing VM for migration: " + std::string(e.what()), 1);
        return false;
    }
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
        memoryNeeded += GetMemory(taskId);
        if (IsGPUCapable(taskId)) {
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
        if (info.machine_id == machineId && !IsVMMigrating(vm) && !VM_IsPendingMigration(vm)) {
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
                    // Prepare VM for migration
                    if (PrepareVMForMigration(vm)) {
                        // Now migrate the VM
                        Machine_MigrateVM(vm, machineId, targetMachine);
                        vmToMachine[vm] = targetMachine;
                        return;
                    }
                } catch (const std::exception& e) {
                    SimOutput("ERROR: Migration failed: " + std::string(e.what()), 1);
                    // Clean up if migration failed
                    migratingVMs.erase(vm);
                }
            }
        }
    }
}

void Scheduler::AdjustMachinePowerState(MachineId_t machineId, double utilization) {
    try {
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        bool hasVMs = false;
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machineId) {
                hasVMs = true;
                break;
            }
        }
        
        if (!hasVMs) {
            Machine_SetState(machineId, S5);
            activeMachines.erase(machineId);
        } else {
            if (utilization < LOW_UTIL_THRESHOLD) {
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machineId, i, P3);
                }
            } else if (utilization < 0.5) {
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machineId, i, P2);
                }
            } else if (utilization < 0.8) {
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machineId, i, P1);
                }
            } else {
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machineId, i, P0);
                }
            }
        }
    } catch (const std::exception& e) {
        SimOutput("ERROR in AdjustMachinePowerState: " + std::string(e.what()), 1);
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
        
        if (!hasVMs) {
            Machine_SetState(machineId, S5);
            activeMachines.erase(machineId);
        }
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    struct TaskInfo_t {
        CPUType_t required_cpu;
        VMType_t required_vm;
        SLAType_t required_sla;
        bool gpu_capable;
        unsigned required_memory;
    } taskInfo;
    
    try {
        taskInfo.required_cpu = ::RequiredCPUType(task_id);
        taskInfo.required_vm = ::RequiredVMType(task_id);
        taskInfo.required_sla = ::RequiredSLA(task_id);
        taskInfo.gpu_capable = ::IsTaskGPUCapable(task_id);
        taskInfo.required_memory = ::GetTaskMemory(task_id);
    } catch (...) {
        taskInfo.required_cpu = RequiredCPUType(task_id);
        taskInfo.required_vm = RequiredVMType(task_id);
        taskInfo.required_sla = RequiredSLA(task_id);
        taskInfo.gpu_capable = IsGPUCapable(task_id);
        taskInfo.required_memory = GetMemory(task_id);
    }
    
    if (!IsCompatibleVMCPU(taskInfo.required_vm, taskInfo.required_cpu)) {
        SimOutput("WARNING: Task " + std::to_string(task_id) + 
                 " requires incompatible VM-CPU combination. Adjusting VM type.", 1);
        
        if (taskInfo.required_cpu == POWER) {
            taskInfo.required_vm = AIX;
        } else if (taskInfo.required_cpu == ARM || taskInfo.required_cpu == X86) {
            if (taskInfo.required_vm == AIX) {
                taskInfo.required_vm = LINUX;
            }
        } else {
            taskInfo.required_vm = LINUX;
        }
    }
    
    Priority_t priority;
    switch (taskInfo.required_sla) {
        case SLA0: priority = HIGH_PRIORITY; break;
        case SLA1: priority = HIGH_PRIORITY; break;
        case SLA2: priority = MID_PRIORITY; break;
        default: priority = LOW_PRIORITY;
    }
    
    std::vector<VMId_t> compatibleVMs = GetCompatibleVMs(taskInfo.required_cpu, taskInfo.required_vm);
    
    VMId_t targetVM = VMId_t(-1);
    
    if (!compatibleVMs.empty()) {
        targetVM = *std::min_element(compatibleVMs.begin(), compatibleVMs.end(),
            [this](VMId_t a, VMId_t b) {
                return VM_GetInfo(a).active_tasks.size() < VM_GetInfo(b).active_tasks.size();
            });
    } else {
        SimOutput("No compatible VM found for task " + std::to_string(task_id) + 
                 ", creating new VM", 2);
        
        MachineId_t targetMachine = MachineId_t(-1);
        
        for (MachineId_t machineId : cpuTypeMachines[taskInfo.required_cpu]) {
            MachineInfo_t info = Machine_GetInfo(machineId);
            
            if (taskInfo.gpu_capable && !info.gpus) continue;
            if (info.memory_used + taskInfo.required_memory > info.memory_size) continue;
            
            targetMachine = machineId;
            
            if (activeMachines.find(machineId) == activeMachines.end()) {
                Machine_SetState(machineId, S0);
                activeMachines.insert(machineId);
            }
            
            break;
        }
        
        if (targetMachine != MachineId_t(-1)) {
            try {
                targetVM = VM_Create(taskInfo.required_vm, taskInfo.required_cpu);
                vms.push_back(targetVM);
                VM_Attach(targetVM, targetMachine);
                vmToMachine[targetVM] = targetMachine;
                
                TaskClass_t taskClass = GetTaskClass(task_id);
                taskClassToVMs[taskClass].push_back(targetVM);
                
                SimOutput("Created new VM " + std::to_string(targetVM) + 
                         " for task " + std::to_string(task_id), 2);
            } catch (const std::exception& e) {
                SimOutput("ERROR: Failed to create VM: " + std::string(e.what()), 1);
                targetVM = VMId_t(-1);
            }
        }
    }
    
    if (targetVM != VMId_t(-1)) {
        try {
            VM_AddTask(targetVM, task_id, priority);
            
            if (taskInfo.required_sla == SLA0 || taskInfo.required_sla == SLA1) {
                MachineId_t machineId = vmToMachine[targetVM];
                MachineInfo_t info = Machine_GetInfo(machineId);
                
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machineId, i, P0);
                }
            }
            
            SimOutput("Added task " + std::to_string(task_id) + 
                     " to VM " + std::to_string(targetVM), 2);
        } catch (const std::exception& e) {
            SimOutput("ERROR: Failed to add task to VM: " + std::string(e.what()), 1);
        }
    } else {
        SimOutput("ERROR: Could not find or create suitable VM for task " + 
                 std::to_string(task_id), 1);
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
    for (VMId_t vm : vms) {
        if (!IsVMMigrating(vm)) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            MachineId_t machineId = vmInfo.machine_id;
            
            double utilization = CalculateMachineUtilization(machineId);
            machineUtilization[machineId] = utilization;
            
            if (utilization < LOW_UTIL_THRESHOLD && vmInfo.active_tasks.size() == 0) {
                AdjustMachinePowerState(machineId, utilization);
            }
        }
    }
    
    if (now - lastConsolidationTime > CONSOLIDATION_INTERVAL) {
        ConsolidateVMs(now);
        lastConsolidationTime = now;
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // VM migration is complete, update state
    VM_MigrationCompleted(vm_id);
    migratingVMs.erase(vm_id);
    
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
        if (IsVMMigrating(vm)) continue;
        
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
        if (IsVMMigrating(vm)) continue;
        
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
        bool alreadyMigrating = false;
        for (VMId_t vm : migratingVMs) {
            VMInfo_t info = VM_GetInfo(vm);
            if (info.machine_id == source) {
                alreadyMigrating = true;
                break;
            }
        }
        if (alreadyMigrating) continue;
        
        if (HasActiveJobs(source)) {
            SimOutput("ConsolidateVMs: Machine " + std::to_string(source) + 
                     " has active jobs, skipping consolidation", 3);
            continue;
        }
        
        std::vector<VMId_t> machineVMs;
        for (VMId_t vm : vms) {
            VMInfo_t info = VM_GetInfo(vm);
            if (info.machine_id == source && !VM_IsPendingMigration(vm) && !IsVMMigrating(vm)) {
                machineVMs.push_back(vm);
            }
        }
        
        if (machineVMs.empty()) {
            SimOutput("ConsolidateVMs: Machine " + std::to_string(source) + 
                     " has no VMs, powering down", 3);
            Machine_SetState(source, S5);
            activeMachines.erase(source);
            continue;
        }
        
        bool allMigrated = true;
        for (VMId_t vm : machineVMs) {
            MachineId_t target = FindMigrationTarget(vm, source);
            if (target != MachineId_t(-1)) {
                if (EnsureMachineAwake(target)) {
                    try {
                        // Prepare VM for migration
                        if (PrepareVMForMigration(vm)) {
                            // Now migrate the VM
                            Machine_MigrateVM(vm, source, target);
                            vmToMachine[vm] = target;
                        } else {
                            allMigrated = false;
                        }
                    } catch (const std::exception& e) {
                        SimOutput("ERROR: Migration failed: " + std::string(e.what()), 1);
                        allMigrated = false;
                        // Clean up if migration failed
                        migratingVMs.erase(vm);
                    }
                } else {
                    allMigrated = false;
                }
            } else {
                allMigrated = false;
                break;
            }
        }
        
        if (allMigrated && !machineVMs.empty()) {
            if (!HasActiveJobs(source)) {
                SimOutput("ConsolidateVMs: All VMs migrated from machine " + 
                         std::to_string(source) + ", powering down", 3);
                Machine_SetState(source, S5);
                activeMachines.erase(source);
            } else {
                SimOutput("WARNING: Machine " + std::to_string(source) + 
                         " still has active jobs after VM migration. Cannot power down.", 2);
            }
        }
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
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}