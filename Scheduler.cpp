//
//  Scheduler.cpp
//  CloudSim
//

#include "Scheduler.hpp"
#include <algorithm>
#include <map>
#include <string>
#include <iostream>

// Initialize static members
const Time_t Scheduler::SLA_THRESHOLD = 100000; // 100ms in microseconds
const double Scheduler::LOAD_THRESHOLD_LOW = 0.3;
const double Scheduler::LOAD_THRESHOLD_HIGH = 0.7;
const unsigned Scheduler::INITIAL_ACTIVE_MACHINES = 12; // Increased from 8 to handle more load initially

// Global scheduler instance
static Scheduler Scheduler;

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing DVFS-based scheduler", 1);
    
    // Get total number of machines
    unsigned total_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(total_machines), 2);
    
    // Initialize data structures
    machine_task_count.resize(total_machines, 0);
    machine_vm_map.resize(total_machines);
    vm_machine_map.clear();
    task_vm_map.clear();
    pending_state_changes.clear();
    pending_migrations.clear();
    pending_tasks.clear();
    
    // Power on initial set of machines and create VMs
    for (unsigned i = 0; i < total_machines; i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        VMType_t vt = LINUX;
        if (info.cpu == X86 || info.cpu == ARM) {
            vt = WIN;
        } else if (info.cpu == POWER) {
            vt = AIX;
        }
        if (i < INITIAL_ACTIVE_MACHINES) {
            // Power on machine and track the pending state change
            VMId_t vm = VM_Create(vt, info.cpu);
            VM_Attach(vm, info.machine_id);
            machine_vm_map[info.machine_id].push_back(vm);
            Machine_SetState(MachineId_t(i), S0);
            pending_state_changes.insert(MachineId_t(i));
            
            // We'll create VMs when the state change is complete
        } else {
            // Power off unused machines
            Machine_SetState(MachineId_t(i), S5);
            SimOutput("Scheduler::Init(): Powered off machine " + to_string(i), 3);
        }
    }
    
    SimOutput("Scheduler::Init(): Scheduler initialized with " + 
              to_string(INITIAL_ACTIVE_MACHINES) + " pending active machines", 1);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task information
    TaskInfo_t task_info = GetTaskInfo(task_id);
    
    // Calculate urgency factor based on deadline
    Time_t time_to_deadline = task_info.target_completion - now;
    uint64_t instructions = task_info.total_instructions;
    double urgency = static_cast<double>(instructions) / time_to_deadline;
    
    SimOutput("Scheduler::NewTask(): New task " + to_string(task_id) + 
              " with urgency factor " + to_string(urgency), 2);
    
    // Try to assign the task immediately for better SLA compliance
    MachineId_t target_machine = FindBestMachine(task_info);
    
    // If no suitable machine found, try to power on a new one
    if (target_machine == INVALID_MACHINE) {
        // Check if we can power on a new machine
        target_machine = PowerOnNewMachine();
        
        if (target_machine == INVALID_MACHINE || 
            pending_state_changes.find(target_machine) != pending_state_changes.end()) {
            // No machines available or machine is powering on
            // Add to pending queue with SLA information
            pending_tasks.push_back({task_id, task_info.required_sla, urgency});
            SimOutput("Scheduler::NewTask(): Added task " + to_string(task_id) + 
                      " to pending queue (size: " + to_string(pending_tasks.size()) + ")", 2);
            return;
        }
    }
    
    // Find or create a VM on the target machine
    VMId_t target_vm = FindOrCreateVM(target_machine, task_info.required_cpu);
    
    // Set the machine's CPU performance based on urgency and SLA
    AdjustMachinePerformance(target_machine, urgency, task_info.required_sla);
    
    // Add the task to the VM with appropriate priority
    Priority_t priority;
    if (task_info.required_sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (task_info.required_sla == SLA1 || urgency > 0.7) {
        priority = HIGH_PRIORITY;
    } else if (task_info.required_sla == SLA2 || urgency > 0.4) {
        priority = MID_PRIORITY;
    } else {
        priority = LOW_PRIORITY;
    }
    
    VM_AddTask(target_vm, task_id, priority);
    
    // Update our mappings
    task_vm_map[task_id] = target_vm;
    machine_task_count[target_machine]++;
    
    SimOutput("Scheduler::NewTask(): Assigned task " + to_string(task_id) + 
              " to VM " + to_string(target_vm) + 
              " on machine " + to_string(target_machine) + 
              " with priority " + to_string(priority), 3);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed", 2);
    
    // Find which VM and machine this task was running on
    auto it = task_vm_map.find(task_id);
    if (it == task_vm_map.end()) {
        SimOutput("Scheduler::TaskComplete(): Warning - Task " + to_string(task_id) + 
                  " not found in our records", 1);
        return;
    }
    
    VMId_t vm_id = it->second;
    MachineId_t machine_id = vm_machine_map[vm_id];
    
    // Remove task from our mapping
    task_vm_map.erase(it);
    
    // Update machine task count
    if (machine_task_count[machine_id] > 0) {
        machine_task_count[machine_id]--;
    }
    
    // Update machine performance based on current load
    UpdateMachinePerformance(machine_id, now);
    
    // Check if we can power off the machine
    CheckMachinePowerState(machine_id);
    
    // Process any pending tasks immediately
    ProcessPendingTasks(now);
}

void Scheduler::PeriodicCheck(Time_t now) {
    SimOutput("Scheduler::PeriodicCheck(): Performing periodic check at time " + 
              to_string(now), 3);
    
    // Iterate through all machines
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        
        // Skip machines that are powered off or in transition
        if (info.s_state == S5 || 
            pending_state_changes.find(MachineId_t(i)) != pending_state_changes.end()) {
            continue;
        }
        
        // Check for SLA violations and adjust performance
        CheckSLAViolations(MachineId_t(i), now);
        
        // Update machine performance based on current load
        UpdateMachinePerformance(MachineId_t(i), now);
        
        // Check if we can power off underutilized machines
        CheckMachinePowerState(MachineId_t(i));
    }
    
    // Check if we need to power on additional machines due to high load
    CheckClusterLoad();
    
    // Process any pending tasks
    ProcessPendingTasks(now);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) + 
              " migration completed at time " + to_string(time), 2);
    
    // Update our data structures
    pending_migrations.erase(vm_id);
    
    // Process any pending tasks that were waiting for this migration
    ProcessPendingTasks(time);
}

void Scheduler::StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("Scheduler::StateChangeComplete(): Machine " + to_string(machine_id) + 
              " state change completed at time " + to_string(time), 2);
    
    // Remove from pending state changes
    pending_state_changes.erase(machine_id);
    
    // If the machine is now powered on (S0), initialize it
    MachineInfo_t info = Machine_GetInfo(machine_id);
    if (info.s_state == S0) {
        // Set all cores to maximum performance initially
        for (unsigned j = 0; j < info.num_cpus; j++) {
            Machine_SetCorePerformance(machine_id, j, P0);
        }
        
        // Create a VM for this machine if it doesn't already have one
        if (machine_vm_map[machine_id].empty()) {
            VMId_t vm_id = VM_Create(LINUX, info.cpu);
            VM_Attach(vm_id, machine_id);
            
            // Update our mappings
            machine_vm_map[machine_id].push_back(vm_id);
            vm_machine_map[vm_id] = machine_id;
            
            SimOutput("Scheduler::StateChangeComplete(): Created VM " + to_string(vm_id) + 
                      " on newly active machine " + to_string(machine_id), 3);
        }
    }
    
    // Process any pending tasks that were waiting for this state change
    ProcessPendingTasks(time);
}

void Scheduler::Shutdown(Time_t now) {
    SimOutput("Scheduler::Shutdown(): Shutting down at time " + to_string(now), 1);
    
    // Shutdown all VMs
    for (auto& vm_pair : vm_machine_map) {
        VM_Shutdown(vm_pair.first);
    }
    
    // Power off all machines
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        Machine_SetState(MachineId_t(i), S5);
    }
    
    SimOutput("Scheduler::Shutdown(): All resources released", 1);
}

void Scheduler::ProcessPendingTasks(Time_t now) {
    // Sort pending tasks by SLA and urgency
    std::sort(pending_tasks.begin(), pending_tasks.end(),
              [](const PendingTask& a, const PendingTask& b) {
                  // First sort by SLA (SLA0 > SLA1 > SLA2 > SLA3)
                  if (a.sla != b.sla) {
                      return a.sla < b.sla;
                  }
                  // Then by urgency
                  return a.urgency > b.urgency;
              });
    
    // Process tasks in order of priority
    auto it = pending_tasks.begin();
    while (it != pending_tasks.end()) {
        TaskId_t task_id = it->task_id;
        
        // Skip tasks that are already assigned
        if (task_vm_map.find(task_id) != task_vm_map.end()) {
            it = pending_tasks.erase(it);
            continue;
        }
        
        // Get task information
        TaskInfo_t task_info = GetTaskInfo(task_id);
        
        // Recalculate urgency based on current time
        Time_t time_to_deadline = task_info.target_completion - now;
        uint64_t instructions = task_info.remaining_instructions;
        double urgency = static_cast<double>(instructions) / time_to_deadline;
        
        // Find the best machine
        MachineId_t target_machine = FindBestMachine(task_info);
        
        // If no suitable machine, try to power on a new one
        if (target_machine == INVALID_MACHINE) {
            target_machine = PowerOnNewMachine();
            
            if (target_machine == INVALID_MACHINE || 
                pending_state_changes.find(target_machine) != pending_state_changes.end()) {
                // No machines available or machine is powering on, skip this task
                ++it;
                continue;
            }
        }
        
        // Check if the machine is ready to accept tasks
        MachineInfo_t machine_info = Machine_GetInfo(target_machine);
        if (machine_info.s_state != S0 || 
            pending_state_changes.find(target_machine) != pending_state_changes.end()) {
            // Machine is not ready, skip this task
            ++it;
            continue;
        }
        
        // Find or create a VM
        VMId_t target_vm = FindOrCreateVM(target_machine, task_info.required_cpu);
        
        // Set machine performance based on urgency and SLA
        AdjustMachinePerformance(target_machine, urgency, task_info.required_sla);
        
        // Add task to VM with appropriate priority
        Priority_t priority;
        if (task_info.required_sla == SLA0) {
            priority = HIGH_PRIORITY;
        } else if (task_info.required_sla == SLA1 || urgency > 0.7) {
            priority = HIGH_PRIORITY;
        } else if (task_info.required_sla == SLA2 || urgency > 0.4) {
            priority = MID_PRIORITY;
        } else {
            priority = LOW_PRIORITY;
        }
        
        VM_AddTask(target_vm, task_id, priority);
        
        // Update mappings
        task_vm_map[task_id] = target_vm;
        machine_task_count[target_machine]++;
        
        SimOutput("Scheduler::ProcessPendingTasks(): Assigned task " + 
                  to_string(task_id) + " to VM " + to_string(target_vm) + 
                  " on machine " + to_string(target_machine) + 
                  " with priority " + to_string(priority), 2);
        
        // Remove from pending queue
        it = pending_tasks.erase(it);
    }
}

MachineId_t Scheduler::FindBestMachine(const TaskInfo_t& task_info) {
    MachineId_t best_machine = INVALID_MACHINE;
    double best_score = std::numeric_limits<double>::max();
    
    // Iterate through all machines
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t machine_info = Machine_GetInfo(MachineId_t(i));
        
        // Skip machines that are powered off or in transition
        if (machine_info.s_state == S5 || 
            pending_state_changes.find(MachineId_t(i)) != pending_state_changes.end()) {
            continue;
        }
        
        // Skip machines with VMs that are being migrated
        bool has_migrating_vm = false;
        for (VMId_t vm_id : machine_vm_map[i]) {
            if (pending_migrations.find(vm_id) != pending_migrations.end() ||
                VM_IsPendingMigration(vm_id)) {
                has_migrating_vm = true;
                break;
            }
        }
        if (has_migrating_vm) continue;
        
        // Check if machine meets requirements
        if (machine_info.cpu != task_info.required_cpu) continue;
        if (task_info.gpu_capable && !machine_info.gpus) continue;
        if (machine_info.memory_used + task_info.required_memory > machine_info.memory_size) continue;
        
        // Calculate a score based on current load and performance state
        double load = static_cast<double>(machine_info.active_tasks) / machine_info.num_cpus;
        double score = load;
        
        // Prefer machines with higher performance states for SLA0 and SLA1 tasks
        if (task_info.required_sla == SLA0 || task_info.required_sla == SLA1) {
            // Prefer machines already at P0 or P1
            if (machine_info.p_state == P0) {
                score -= 0.3;
            } else if (machine_info.p_state == P1) {
                score -= 0.2;
            }
        }
        
        // Prefer machines that are underutilized
        if (load < LOAD_THRESHOLD_LOW) {
            score -= 0.2;
        }
        
        // Update best machine if this one has a better score
        if (score < best_score) {
            best_score = score;
            best_machine = MachineId_t(i);
        }
    }
    
    return best_machine;
}

MachineId_t Scheduler::PowerOnNewMachine() {
    // Find a powered-off machine that's not in transition
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        
        if (info.s_state == S5 && 
            pending_state_changes.find(MachineId_t(i)) == pending_state_changes.end()) {
            // Power on the machine
            Machine_SetState(MachineId_t(i), S0);
            
            // Mark as pending state change
            pending_state_changes.insert(MachineId_t(i));
            
            SimOutput("Scheduler::PowerOnNewMachine(): Powering on machine " + 
                      to_string(i), 2);
            
            return MachineId_t(i);
        }
    }
    
    return INVALID_MACHINE;
}

VMId_t Scheduler::FindOrCreateVM(MachineId_t machine_id, CPUType_t cpu_type) {
    // Check if there's an existing VM on this machine that's not migrating
    for (VMId_t vm_id : machine_vm_map[machine_id]) {
        if (pending_migrations.find(vm_id) == pending_migrations.end() &&
            !VM_IsPendingMigration(vm_id)) {
            return vm_id;
        }
    }
    
    // Create a new VM
    VMId_t vm_id = VM_Create(LINUX, cpu_type);
    VM_Attach(vm_id, machine_id);
    
    // Update our mappings
    machine_vm_map[machine_id].push_back(vm_id);
    vm_machine_map[vm_id] = machine_id;
    
    SimOutput("Scheduler::FindOrCreateVM(): Created new VM " + to_string(vm_id) + 
              " on machine " + to_string(machine_id), 3);
    
    return vm_id;
}

void Scheduler::AdjustMachinePerformance(MachineId_t machine_id, double urgency, SLAType_t sla) {
    // Skip machines in transition
    if (pending_state_changes.find(machine_id) != pending_state_changes.end()) {
        return;
    }
    
    MachineInfo_t info = Machine_GetInfo(machine_id);
    CPUPerformance_t target_state;
    
    // Set performance based on SLA and urgency
    if (sla == SLA0 || urgency > 0.8) {
        target_state = P0;  // SLA0 or high urgency - maximum performance
    } else if (sla == SLA1 || urgency > 0.6) {
        target_state = P0;  // SLA1 or medium-high urgency - high performance
    } else if (sla == SLA2 || urgency > 0.4) {
        target_state = P1;  // SLA2 or medium urgency
    } else if (urgency > 0.2) {
        target_state = P2;  // Low urgency
    } else {
        target_state = P3;  // Very low urgency - minimum performance
    }
    
    // Only change if different from current state
    if (info.p_state != target_state) {
        // Apply the performance state to all cores
        for (unsigned j = 0; j < info.num_cpus; j++) {
            Machine_SetCorePerformance(machine_id, j, target_state);
        }
        
        SimOutput("Scheduler::AdjustMachinePerformance(): Set machine " + 
                  to_string(machine_id) + " to P-state " + to_string(target_state), 3);
    }
}

void Scheduler::UpdateMachinePerformance(MachineId_t machine_id, Time_t now) {
    // Skip machines in transition
    if (pending_state_changes.find(machine_id) != pending_state_changes.end()) {
        return;
    }
    
    MachineInfo_t info = Machine_GetInfo(machine_id);
    
    // Calculate load factor
    double load = static_cast<double>(info.active_tasks) / info.num_cpus;
    
    // Check for SLA violations first
    bool has_urgent_tasks = CheckSLAViolations(machine_id, now);
    
    // If no urgent tasks, adjust based on load
    if (!has_urgent_tasks) {
        CPUPerformance_t target_state;
        
        if (load > LOAD_THRESHOLD_HIGH) {
            target_state = P0;  // High load - maximum performance
        } else if (load > LOAD_THRESHOLD_LOW) {
            target_state = P1;  // Medium load
        } else if (load > 0.1) {
            target_state = P2;  // Low load
        } else {
            target_state = P3;  // Very low load - minimum performance
        }
        
        // Only change if different from current state
        if (info.p_state != target_state) {
            // Apply the performance state to all cores
            for (unsigned j = 0; j < info.num_cpus; j++) {
                Machine_SetCorePerformance(machine_id, j, target_state);
            }
            
            SimOutput("Scheduler::UpdateMachinePerformance(): Updated machine " + 
                      to_string(machine_id) + " to P-state " + to_string(target_state) + 
                      " based on load " + to_string(load), 3);
        }
    }
}

bool Scheduler::CheckSLAViolations(MachineId_t machine_id, Time_t now) {
    // Skip machines in transition
    if (pending_state_changes.find(machine_id) != pending_state_changes.end()) {
        return false;
    }
    
    bool has_urgent_tasks = false;
    
    // Check all VMs on this machine
    for (VMId_t vm_id : machine_vm_map[machine_id]) {
        // Skip VMs that are being migrated
        if (pending_migrations.find(vm_id) != pending_migrations.end() ||
            VM_IsPendingMigration(vm_id)) {
            continue;
        }
        
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        
        // Check all tasks on this VM
        for (TaskId_t task_id : vm_info.active_tasks) {
            TaskInfo_t task_info = GetTaskInfo(task_id);
            
            // Check if this task is in danger of violating its SLA
            Time_t time_to_deadline = task_info.target_completion - now;
            uint64_t remaining = GetRemainingInstructions(task_id);
            
            // Calculate required MIPS to meet deadline
            double required_mips = static_cast<double>(remaining) / time_to_deadline;
            
            // Get machine's current MIPS rating
            MachineInfo_t machine_info = Machine_GetInfo(machine_id);
            double current_mips = machine_info.performance[machine_info.p_state];
            
            // Calculate SLA factor based on SLA type
            double sla_factor;
            switch (task_info.required_sla) {
                case SLA0: sla_factor = 0.85; break; // More aggressive for SLA0
                case SLA1: sla_factor = 0.9; break;
                case SLA2: sla_factor = 0.95; break;
                default: sla_factor = 1.0; break;
            }
            
            // If we need more performance to meet deadline
            if (required_mips > current_mips * sla_factor) {
                has_urgent_tasks = true;
                
                // Only change if not already at P0
                if (machine_info.p_state != P0) {
                    // Set machine to maximum performance
                    for (unsigned j = 0; j < machine_info.num_cpus; j++) {
                        Machine_SetCorePerformance(machine_id, j, P0);
                    }
                    
                    SimOutput("Scheduler::CheckSLAViolations(): Boosted machine " + 
                              to_string(machine_id) + " to P0 for task " + 
                              to_string(task_id) + " to avoid SLA violation", 2);
                }
                
                break;  // No need to check other tasks
            }
        }
        
        if (has_urgent_tasks) break;  // No need to check other VMs
    }
    
    return has_urgent_tasks;
}

void Scheduler::CheckMachinePowerState(MachineId_t machine_id) {
    // Skip machines in transition
    if (pending_state_changes.find(machine_id) != pending_state_changes.end()) {
        return;
    }
    
    MachineInfo_t info = Machine_GetInfo(machine_id);
    
    // If machine has no tasks, consider powering it off
    if (info.active_tasks == 0) {
        // Make sure we keep a minimum number of machines running
        unsigned active_count = 0;
        for (unsigned i = 0; i < Machine_GetTotal(); i++) {
            MachineInfo_t m_info = Machine_GetInfo(MachineId_t(i));
            if (m_info.s_state != S5 && 
                pending_state_changes.find(MachineId_t(i)) == pending_state_changes.end()) {
                active_count++;
            }
        }
        
        if (active_count > INITIAL_ACTIVE_MACHINES) {
            // Check if any VMs on this machine are being migrated
            bool has_migrating_vm = false;
            for (VMId_t vm_id : machine_vm_map[machine_id]) {
                if (pending_migrations.find(vm_id) != pending_migrations.end() ||
                    VM_IsPendingMigration(vm_id)) {
                    has_migrating_vm = true;
                    break;
                }
            }
            
            if (!has_migrating_vm) {
                // Shutdown all VMs on this machine first
                for (VMId_t vm_id : machine_vm_map[machine_id]) {
                    VM_Shutdown(vm_id);
                    vm_machine_map.erase(vm_id);
                }
                machine_vm_map[machine_id].clear();
                
                // Power off the machine
                Machine_SetState(machine_id, S5);
                pending_state_changes.insert(machine_id);
                
                SimOutput("Scheduler::CheckMachinePowerState(): Powering off idle machine " + 
                          to_string(machine_id), 2);
            }
        }
    }
}

void Scheduler::CheckClusterLoad() {
    // Calculate overall cluster load
    unsigned total_active_tasks = 0;
    unsigned total_active_cores = 0;
    
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        
        // Only count machines that are active and not in transition
        if (info.s_state != S5 && 
            pending_state_changes.find(MachineId_t(i)) == pending_state_changes.end()) {
            total_active_tasks += info.active_tasks;
            total_active_cores += info.num_cpus;
        }
    }
    
    // Avoid division by zero
    if (total_active_cores == 0) return;
    
    double cluster_load = static_cast<double>(total_active_tasks) / total_active_cores;
    
    // If cluster load is high or we have pending tasks, consider powering on more machines
    if (cluster_load > LOAD_THRESHOLD_HIGH * 0.8 || !pending_tasks.empty()) { // More aggressive threshold
        PowerOnNewMachine();
        
        SimOutput("Scheduler::CheckClusterLoad(): Powered on additional machine due to high cluster load " + 
                  to_string(cluster_load) + " or pending tasks", 2);
    }
}

// Public interface functions called by the simulator

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + 
              " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + 
              " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Memory warning on machine " + to_string(machine_id) + 
              " at time " + to_string(time), 2);
    
    // Could implement VM migration or task reallocation here
    // For now, just log the warning
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): VM " + to_string(vm_id) + 
              " migration completed at time " + to_string(time), 3);
    Scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): Periodic check at time " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    SimOutput("SimulationComplete(): Simulation completed at time " + to_string(time), 2);
    
    // Report final statistics
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    
    // Shutdown the scheduler
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): SLA warning for task " + to_string(task_id) + 
              " at time " +  to_string(time), 2);
    
    // Find which VM and machine this task is running on
    auto it = Scheduler.task_vm_map.find(task_id);
    if (it != Scheduler.task_vm_map.end()) {
        VMId_t vm_id = it->second;
        MachineId_t machine_id = Scheduler.vm_machine_map[vm_id];
        
        // Skip machines in transition
        if (Scheduler.pending_state_changes.find(machine_id) != Scheduler.pending_state_changes.end()) {
            return;
        }
        
        // Get current machine info
        MachineInfo_t info = Machine_GetInfo(machine_id);
        
        // Only boost if not already at P0
        if (info.p_state != P0) {
            // Boost the machine to maximum performance
            for (unsigned j = 0; j < info.num_cpus; j++) {
                Machine_SetCorePerformance(machine_id, j, P0);
            }
            
            SimOutput("SLAWarning(): Boosted machine " + to_string(machine_id) + 
                      " to P0 for task " + to_string(task_id), 2);
        } else {
            SimOutput("SLAWarning(): Machine " + to_string(machine_id) + 
                      " already at P0 for task " + to_string(task_id), 3);
        }
        
        // Also try to prioritize this task
        TaskInfo_t task_info = GetTaskInfo(task_id);
        if (task_info.required_sla == SLA0 || task_info.required_sla == SLA1) {
            SetTaskPriority(task_id, HIGH_PRIORITY);
            SimOutput("SLAWarning(): Set task " + to_string(task_id) + 
                      " to HIGH_PRIORITY", 2);
        }
    } else {
        SimOutput("SLAWarning(): Task " + to_string(task_id) + 
                  " not found in our records, ignoring SLA warning", 2);
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + 
              " state change completed at time " + to_string(time), 3);
    
    Scheduler.StateChangeComplete(time, machine_id);
}