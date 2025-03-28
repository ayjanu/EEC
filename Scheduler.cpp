//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include "Internal_Interfaces.h"

static unsigned active_machines = 16;
Scheduler scheduler;

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    
    // Initialize tracking structures
    last_energy_check = 0;
    last_cluster_energy = 0;
    
    // Get total number of machines
    unsigned total_machines = Machine_GetTotal();
    active_machines = total_machines;
    
    // Store machine IDs and gather machine information
    for(unsigned i = 0; i < total_machines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machine_has_gpu[i] = info.gpus;
        machine_states[i] = S0; // Start all machines in active state
        machine_cpus[i] = info.cpu;
    }
    
    // Create VMs with compatible CPU types for each active machine
    for(unsigned i = 0; i < active_machines; i++) {
        MachineId_t machine_id = machines[i];
        // Create a VM with compatible CPU type
        VMType_t vm_type = LINUX_RT; // Default VM type
        if (machine_cpus[i] == ARM || machine_cpus[i] == X86) {
            vm_type = WIN;
        } else if (machine_cpus[i] == POWER) {
            vm_type = AIX;
        }
        // Create VM with the same CPU type as the machine
        VMId_t vm = VM_Create(vm_type, machine_cpus[i]);
        vms.push_back(vm);
        VM_Attach(vm, machine_id);
        
        // Initialize load tracking
        vm_load[vm] = 0;

        VMId_t vm2 = VM_Create(vm_type, machine_cpus[i]);
        vms.push_back(vm2);
        VM_Attach(vm2, machine_id);
        
        // Initialize load tracking
        vm_load[vm2] = 0;
        
        SimOutput("Scheduler::Init(): Created VM " + to_string(vm) + 
                  " with CPU type " + to_string(machine_cpus[i]) + 
                  " attached to machine " + to_string(machine_id), 3);
    }
    
    // Turn off machines that aren't being used initially to save power
    for(unsigned i = active_machines; i < total_machines; i++) {
        Machine_SetState(machines[i], S3);
        machine_states[machines[i]] = S3;
        SimOutput("Scheduler::Init(): Setting unused machine " + to_string(machines[i]) + " to S3 state", 2);
    }
    
    SimOutput("Scheduler::Init(): Initialized " + to_string(vms.size()) + " VMs on compatible machines", 2);
    
    // Set initial P-states for active machines to balance performance and energy
    for(unsigned i = 0; i < active_machines; i++) {
        MachineId_t machine_id = machines[i];
        MachineInfo_t info = Machine_GetInfo(machine_id);
        
        // Set all cores to a moderate performance level initially
        for(unsigned core = 0; core < info.num_cpus; core++) {
            Machine_SetCorePerformance(machine_id, core, P1);
        }
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task information - assuming there's a function to get task info
    TaskInfo_t task_info = GetTaskInfo(task_id);
    
    // Store task metadata for future reference
    task_sla[task_id] = task_info.required_sla;
    task_arrival[task_id] = now;
    
    // Find the best VM for this task based on current load and task requirements
    VMId_t selected_vm = findBestVMForTask(task_id);
    
    // Assign task to selected VM with appropriate priority
    Priority_t priority = (task_info.required_sla == SLA0 || task_info.required_sla == SLA1) ? HIGH_PRIORITY : MID_PRIORITY;
    VM_AddTask(selected_vm, task_id, priority);
    
    // Update our tracking structures
    vm_load[selected_vm]++;
    vm_tasks[selected_vm].push_back(task_id);
    task_to_vm[task_id] = selected_vm;
    
    SimOutput("Scheduler::NewTask(): Assigned task " + to_string(task_id) + 
              " to VM " + to_string(selected_vm) + 
              " (SLA: " + to_string(task_info.required_sla) + ")", 2);
}

VMId_t Scheduler::findBestVMForTask(TaskId_t task_id) {
    // Priority queue to rank VMs by suitability (lower score is better)
    priority_queue<pair<int, VMId_t>, vector<pair<int, VMId_t>>, greater<pair<int, VMId_t>>> candidates;
    
    // Get task info
    TaskInfo_t task_info = GetTaskInfo(task_id);
    bool needs_gpu = task_info.gpu_capable;
    unsigned task_memory = task_info.required_memory;
    CPUType_t ctype = task_info.required_cpu;
    
    for (size_t i = 0; i < vms.size(); i++) {
        VMId_t vm = vms[i];
        // Skip if VM is migrating
        if (VM_IsPendingMigration(vm)) continue;
        
        // Get VM info to find its machine
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.cpu != ctype) continue; 
        MachineId_t machine = vm_info.machine_id;
        
        // Base score is current load
        int score = vm_load[vm] * 10;
        
        // If task needs GPU but machine doesn't have one, penalize heavily
        if (needs_gpu && !machine_has_gpu[machine]) {
            score += 1000;
        }
        
        // Penalize machines in power saving modes
        if (machine_states[machine] != S0) {
            score += 50;
        }
        
        // Check if machine has enough memory
        MachineInfo_t machine_info = Machine_GetInfo(machine);
        unsigned available_memory = machine_info.memory_size - machine_info.memory_used;
        if (available_memory < task_memory) {
            score += 500;
        }
        
        candidates.push(make_pair(score, vm));
    }
    
    // Return the best VM (if any available)
    if (!candidates.empty()) {
        return candidates.top().second;
    }
    
    // Fallback to round-robin if no suitable VM found
    static size_t next_vm = 0;
    VMId_t selected = vms[next_vm];
    next_vm = (next_vm + 1) % vms.size();
    return selected;
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Find which VM was running this task
    if (task_to_vm.find(task_id) == task_to_vm.end()) {
        SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " not found in tracking", 1);
        return;
    }
    
    VMId_t vm = task_to_vm[task_id];
    
    // Update load tracking
    vm_load[vm]--;
    
    // Remove task from VM's task list
    auto& tasks = vm_tasks[vm];
    tasks.erase(remove(tasks.begin(), tasks.end(), task_id), tasks.end());
    
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + 
              " completed on VM " + to_string(vm), 2);
    
    // Clean up tracking
    task_to_vm.erase(task_id);
    task_sla.erase(task_id);
    task_arrival.erase(task_id);
    
    // Adjust machine states if needed
    if (vm_load[vm] == 0) {
        // Consider putting machine to sleep if no tasks
        VMInfo_t vm_info = VM_GetInfo(vm);
        MachineId_t machine = vm_info.machine_id;
        
        // Check if other VMs are using this machine
        bool machine_in_use = false;
        for (auto& vm_pair : vm_load) {
            if (vm_pair.first != vm) {
                VMInfo_t other_vm_info = VM_GetInfo(vm_pair.first);
                if (other_vm_info.machine_id == machine && vm_pair.second > 0) {
                    machine_in_use = true;
                    break;
                }
            }
        }
        
        // if (!machine_in_use) {
        //     // Put machine into low power state
        //     Machine_SetState(machine, S3);
        //     machine_states[machine] = S3;
        //     SimOutput("Scheduler::TaskComplete(): Setting machine " + to_string(machine) + " to S3 state", 2);
        // }
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Check energy consumption periodically
    if (now - last_energy_check > 5000000) { // Every 5 seconds
        double current_energy = Machine_GetClusterEnergy();
        double energy_delta = current_energy - last_cluster_energy;
        
        SimOutput("Scheduler::PeriodicCheck(): Cluster energy consumption: " + 
                  to_string(energy_delta) + " since last check", 3);
        
        last_cluster_energy = current_energy;
        last_energy_check = now;
    }
    
    // Adjust machine states based on current load
    adjustMachineStates();
    
    // Consider VM migrations to balance load
    // migrateVMIfNeeded(now);
}

void Scheduler::adjustMachineStates() {
    // Count active tasks per machine
    map<MachineId_t, unsigned> machine_load;
    
    for (auto& vm_pair : vm_load) {
        VMId_t vm = vm_pair.first;
        unsigned load = vm_pair.second;
        
        VMInfo_t vm_info = VM_GetInfo(vm);
        MachineId_t machine = vm_info.machine_id;
        
        machine_load[machine] += load;
    }
    
    // Adjust machine states based on load
    for (auto& machine_pair : machine_load) {
        MachineId_t machine = machine_pair.first;
        unsigned load = machine_pair.second;
        
        // if (load == 0 && machine_states[machine] == S0) {
        //     // No tasks, put machine to sleep
        //     Machine_SetState(machine, S3);
        //     machine_states[machine] = S3;
        //     SimOutput("Scheduler::adjustMachineStates(): Setting machine " + to_string(machine) + " to S3 state", 2);
        // }
        // else 
        if (load > 0 && machine_states[machine] != S0) {
            // Has tasks but not fully active, wake up
            Machine_SetState(machine, S0);
            machine_states[machine] = S0;
            SimOutput("Scheduler::adjustMachineStates(): Setting machine " + to_string(machine) + " to S0 state", 2);
        }
    }
}

void Scheduler::migrateVMIfNeeded(Time_t now) {
    
    // Find overloaded and underloaded machines
    vector<MachineId_t> overloaded;
    vector<MachineId_t> underloaded;
    
    map<MachineId_t, unsigned> machine_load;
    
    // Calculate load per machine
    for (auto& vm_pair : vm_load) {
        VMId_t vm = vm_pair.first;
        unsigned load = vm_pair.second;
        
        VMInfo_t vm_info = VM_GetInfo(vm);
        MachineId_t machine = vm_info.machine_id;
        
        machine_load[machine] += load;
    }
    
    // Determine average load
    unsigned total_load = 0;
    unsigned active_machine_count = 0;
    for (auto& load_pair : machine_load) {
        total_load += load_pair.second;
        if (machine_states[load_pair.first] == S0) {
            active_machine_count++;
        }
    }
    
    double avg_load = (active_machine_count > 0) ? total_load / (double)active_machine_count : 0;
    
    // Classify machines
    for (auto& load_pair : machine_load) {
        MachineId_t machine = load_pair.first;
        unsigned load = load_pair.second;
        
        // Only consider active machines
        if (machine_states[machine] != S0) {
            continue;
        }
        
        if (load > avg_load * 1.5) {
            overloaded.push_back(machine);
        }
        else if (load < avg_load * 0.5 && load > 0) {
            underloaded.push_back(machine);
        }
    }
    
    // Attempt migrations from overloaded to underloaded
    for (MachineId_t src : overloaded) {
        if (underloaded.empty()) break;
        
        // Find VMs on this machine
        vector<VMId_t> candidate_vms;
        for (VMId_t vm : vms) {
            VMInfo_t vm_info = VM_GetInfo(vm);
            if (vm_info.machine_id == src && !VM_IsPendingMigration(vm)) {
                candidate_vms.push_back(vm);
            }
        }
        
        // Sort VMs by load (descending)
        sort(candidate_vms.begin(), candidate_vms.end(), 
             [this](VMId_t a, VMId_t b) { return vm_load[a] > vm_load[b]; });
        
        // Try to migrate a VM
        for (VMId_t vm : candidate_vms) {
            // Get VM's CPU type
            VMInfo_t vm_info = VM_GetInfo(vm);
            CPUType_t vm_cpu_type = vm_info.cpu;
            
            // Calculate VM memory requirements
            unsigned vm_memory = 0;
            for (TaskId_t task : vm_tasks[vm]) {
                TaskInfo_t task_info = GetTaskInfo(task);
                vm_memory += task_info.required_memory;
            }
            
            // Find compatible underloaded machines
            for (auto it = underloaded.begin(); it != underloaded.end(); ) {
                MachineId_t dest = *it;
                
                // Check CPU compatibility
                CPUType_t dest_cpu_type = Machine_GetCPUType(dest);
                if (vm_cpu_type != dest_cpu_type) {
                    // CPU types are incompatible, try next machine
                    ++it;
                    continue;
                }
                
                // Check if destination has enough resources
                MachineInfo_t dest_info = Machine_GetInfo(dest);
                unsigned available_memory = dest_info.memory_size - dest_info.memory_used;
                
                // Check if GPU requirements match
                bool vm_needs_gpu = false;
                for (TaskId_t task : vm_tasks[vm]) {
                    TaskInfo_t task_info = GetTaskInfo(task);
                    if (task_info.gpu_capable) {
                        vm_needs_gpu = true;
                        break;
                    }
                }
                
                // Skip if VM needs GPU but destination doesn't have one
                if (vm_needs_gpu && !dest_info.gpus) {
                    ++it;
                    continue;
                }
                
                if (available_memory >= vm_memory + VM_MEMORY_OVERHEAD && !VM_IsPendingMigration(vm)) {
                    // Initiate migration
                    VM_MigrationStarted(vm);
                    VM_Migrate(vm, dest);
                    SimOutput("Scheduler::migrateVMIfNeeded(): Migrating VM " + to_string(vm) + 
                              " from machine " + to_string(src) + 
                              " to " + to_string(dest) + 
                              " (CPU type: " + to_string(dest_cpu_type) + ")", 2);
                    
                    // Remove destination from underloaded list
                    underloaded.erase(it);
                    
                    // Successfully migrated a VM, break out of the loop
                    return;
                }
                
                // This machine wasn't suitable, try the next one
                ++it;
            }
        }
    }
    
    // If we have idle machines in S3 state and overloaded machines, consider waking up a machine
    if (!overloaded.empty()) {
        for (MachineId_t machine : machines) {
            if (machine_states[machine] == S3) {
                // Wake up this machine
                Machine_SetState(machine, S0);
                machine_states[machine] = S0;
                SimOutput("Scheduler::migrateVMIfNeeded(): Waking up machine " + to_string(machine) + 
                          " to handle load imbalance", 2);
                break;
            }
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) + 
              " migration completed at time " + to_string(time), 2);
    
    VM_MigrationCompleted(vm_id);
}

void Scheduler::MemoryWarningHandler(Time_t time, MachineId_t machine_id) {
    SimOutput("Scheduler::MemoryWarningHandler(): Memory warning on machine " + 
              to_string(machine_id) + " at time " + to_string(time), 1);
    
    // Find VMs on this machine
    vector<VMId_t> machine_vms;
    for (VMId_t vm : vms) {
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.machine_id == machine_id) {
            machine_vms.push_back(vm);
        }
    }
    
    // Sort VMs by memory usage (descending)
    sort(machine_vms.begin(), machine_vms.end(), 
         [this](VMId_t a, VMId_t b) {
             unsigned mem_a = 0, mem_b = 0;
             for (TaskId_t task : vm_tasks[a]) {
                 TaskInfo_t task_info = GetTaskInfo(task);
                 mem_a += task_info.required_memory;
             }
             for (TaskId_t task : vm_tasks[b]) {
                 TaskInfo_t task_info = GetTaskInfo(task);
                 mem_b += task_info.required_memory;
             }
             return mem_a > mem_b;
         });
    
    // Find a target machine with available memory
    for (VMId_t vm : machine_vms) {
        if (VM_IsPendingMigration(vm)) continue;
        // Calculate VM memory usage
        unsigned vm_memory = 0;
        for (TaskId_t task : vm_tasks[vm]) {
            TaskInfo_t task_info = GetTaskInfo(task);
            vm_memory += task_info.required_memory;
        }
        
        // Find a suitable destination
        for (MachineId_t dest : machines) {
            // if (dest == machine_id) continue;
            
            MachineInfo_t dest_info = Machine_GetInfo(dest);
            unsigned available_memory = dest_info.memory_size - dest_info.memory_used;
            
            if (available_memory >= vm_memory + VM_MEMORY_OVERHEAD) {
                // Migrate VM to destination
                VM_MigrationStarted(vm);
                VM_Migrate(vm, dest);
                SimOutput("Scheduler::MemoryWarningHandler(): Migrating VM " + to_string(vm) + 
                          " from machine " + to_string(machine_id) + " to " + to_string(dest), 2);
                return;
            }
        }
    }
    
    SimOutput("Scheduler::MemoryWarningHandler(): No suitable migration target found", 1);
}

void Scheduler::Shutdown(Time_t now) {
    SimOutput("Scheduler::Shutdown(): Shutting down at time " + to_string(now), 1);
    
    // Final energy report
    double total_energy = Machine_GetClusterEnergy();
    SimOutput("Scheduler::Shutdown(): Total energy consumed: " + to_string(total_energy), 1);
}

TaskClass_t GetTaskClass(TaskId_t task_id) {
    // Get task information
    TaskInfo_t task_info = GetTaskInfo(task_id);
    
    // Get remaining instructions as a measure of task length
    uint64_t remaining_inst = task_info.remaining_instructions;
    
    // Check if task can benefit from GPU
    bool gpu_capable = IsTaskGPUCapable(task_id);
    
    // Check memory requirements (higher memory might indicate compute-intensive tasks)
    unsigned memory_req = GetTaskMemory(task_id);
    
    // Determine task class based on heuristics
    
    // AI: Compute-intensive and can benefit from GPU
    if (gpu_capable && memory_req > 1024 && remaining_inst > 1000000) {
        return AI_TRAINING;
    }
    
    // CRYPTO: Compute-intensive, short, but repetitive, GPU-enabled
    if (gpu_capable && remaining_inst < 500000 && memory_req < 512) {
        return CRYPTO;
    }
    
    // HPC (Scientific): Compute-intensive, very long, can benefit from GPU
    if (gpu_capable && remaining_inst > 5000000) {
        return SCIENTIFIC;
    }
    
    // STREAMING: Compute-intensive, short bursts
    if (remaining_inst > 100000 && remaining_inst < 1000000 && memory_req > 512) {
        return STREAMING;
    }
    
    // WEB: Short requests (default case)
    return WEB_REQUEST;
}

// External interface functions that will be called by the simulator
void InitScheduler() {
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    scheduler.TaskComplete(time, task_id);
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // scheduler.MemoryWarningHandler(time, machine_id);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    scheduler.MigrationComplete(time, vm_id);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): SLA warning for task " + to_string(task_id) + " at time " + to_string(time), 2);
    // Boost priority for this task if we're tracking it
    if (scheduler.task_to_vm.find(task_id) != scheduler.task_to_vm.end()) {
        SetTaskPriority(task_id, HIGH_PRIORITY);
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " state change completed at time " + to_string(time), 3);
}