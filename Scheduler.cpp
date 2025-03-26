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
static bool migrating = false;

// Logging helper function with timestamps
void LogMessage(const std::string &message, int level)
{
    // Direct console output for critical messages
    if (level <= 1)
    {
        std::cout << message << std::endl;
        std::cout.flush();
    }

    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << "[" << std::put_time(std::localtime(&now_time), "%H:%M:%S") << "] ";
    ss << message;

    SimOutput(ss.str(), level);
}

// Global interface functions that the simulator calls
void InitScheduler()
{
    std::cout << "DIRECT OUTPUT: InitScheduler starting" << std::endl;
    std::cout.flush();

    LogMessage("InitScheduler(): Starting initialization", 2);
    scheduler.Init();
    LogMessage("InitScheduler(): Initialization complete", 2);
}

void HandleNewTask(Time_t time, TaskId_t task_id)
{
    // LogMessage("HandleNewTask(): Received new task " + to_string(task_id) +
    //   " at time " + to_string(time), 2);

    scheduler.NewTask(time, task_id);

    // LogMessage("HandleNewTask(): Task " + to_string(task_id) + " processed", 2);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id)
{
    // LogMessage("HandleTaskCompletion(): Task " + to_string(task_id) +
    //   " completed at time " + to_string(time), 2);

    scheduler.TaskComplete(time, task_id);

    // LogMessage("HandleTaskCompletion(): Task completion processed", 2);
}

void MemoryWarning(Time_t time, MachineId_t machine_id)
{
    LogMessage("MemoryWarning(): Memory overflow detected on machine " +
                   to_string(machine_id) + " at time " + to_string(time),
               1);

    // Get machine information
    MachineInfo_t machineInfo = Machine_GetInfo(machine_id);

    // Log detailed memory information
    LogMessage("MemoryWarning(): Machine " + to_string(machine_id) +
                   " memory usage is critical",
               2);

    // 1. First approach: Find VMs on this machine
    std::vector<VMId_t> vmsOnMachine;
    std::map<VMId_t, unsigned> vmTaskCount;

    for (VMId_t vm : scheduler.GetVMs())
    {
        VMInfo_t vmInfo = VM_GetInfo(vm);

        if (vmInfo.machine_id == machine_id)
        {
            vmsOnMachine.push_back(vm);
            vmTaskCount[vm] = vmInfo.active_tasks.size();

            LogMessage("MemoryWarning(): VM " + to_string(vm) +
                           " has " + to_string(vmInfo.active_tasks.size()) +
                           " active tasks",
                       2);
        }
    }

    // 2. Identify the VM with the most tasks (as a proxy for memory usage)
    VMId_t largestVM = VMId_t(-1);
    unsigned mostTasks = 0;

    for (const auto &pair : vmTaskCount)
    {
        if (pair.second > mostTasks)
        {
            mostTasks = pair.second;
            largestVM = pair.first;
        }
    }

    // 3. Try to migrate the VM with the most tasks to another machine
    if (largestVM != VMId_t(-1))
    {
        LogMessage("MemoryWarning(): Attempting to migrate VM " +
                       to_string(largestVM) + " with " + to_string(mostTasks) +
                       " tasks",
                   1);

        VMInfo_t vmInfo = VM_GetInfo(largestVM);
        CPUType_t vmCpuType = vmInfo.cpu;

        // Find a suitable target machine
        MachineId_t targetMachine = MachineId_t(-1);

        for (MachineId_t machine : scheduler.GetMachines())
        {
            // Skip the current machine
            if (machine == machine_id)
                continue;

            // Skip machines that are powered off
            if (scheduler.IsMachineActive(machine) == false)
                continue;

            MachineInfo_t info = Machine_GetInfo(machine);

            // Check CPU compatibility
            if (info.cpu != vmCpuType)
                continue;

            // Check if the machine has few or no tasks
            if (info.active_tasks < 2)
            { // Machine has 0 or 1 active tasks
                targetMachine = machine;
                LogMessage("MemoryWarning(): Found suitable target machine " +
                               to_string(targetMachine) + " with " +
                               to_string(info.active_tasks) + " active tasks",
                           2);
                break;
            }
        }

        // If no suitable active machine, try to power on a machine
        if (targetMachine == MachineId_t(-1))
        {
            for (MachineId_t machine : scheduler.GetMachines())
            {
                // Skip active machines
                if (scheduler.IsMachineActive(machine))
                    continue;

                MachineInfo_t info = Machine_GetInfo(machine);

                // Check CPU compatibility
                if (info.cpu != vmCpuType)
                    continue;

                // Power on the machine
                LogMessage("MemoryWarning(): Powering on machine " +
                               to_string(machine) + " for VM migration",
                           1);

                Machine_SetState(machine, S0);
                scheduler.ActivateMachine(machine);

                targetMachine = machine;
                break;
            }
        }

        // Perform the migration if a target was found
        if (targetMachine != MachineId_t(-1))
        {
            LogMessage("MemoryWarning(): Migrating VM " + to_string(largestVM) +
                           " from machine " + to_string(machine_id) +
                           " to machine " + to_string(targetMachine),
                       1);
            scheduler.MarkVMAsMigrating(largestVM);
            VM_Migrate(largestVM, targetMachine);
        }
        else
        {
            LogMessage("MemoryWarning(): No suitable target machine found for VM migration", 1);

            // 4. If migration is not possible, try to identify and remove a low priority task
            // Since we don't have Task_GetInfo, we'll use a simpler approach

            // Find a VM with tasks
            for (VMId_t vm : vmsOnMachine)
            {
                VMInfo_t vmInfo = VM_GetInfo(vm);

                if (!vmInfo.active_tasks.empty())
                {
                    // Remove the last task (assuming it might be lower priority)
                    TaskId_t taskToRemove = vmInfo.active_tasks.back();

                    LogMessage("MemoryWarning(): Removing task " +
                                   to_string(taskToRemove) + " from VM " +
                                   to_string(vm) + " to free memory",
                               1);

                    VM_RemoveTask(vm, taskToRemove);
                    break;
                }
            }
        }
    }
    else
    {
        LogMessage("MemoryWarning(): No VMs found on machine " +
                       to_string(machine_id),
                   1);
    }

    // 5. As a last resort, adjust machine power state to try to recover
    LogMessage("MemoryWarning(): Setting machine " + to_string(machine_id) +
                   " cores to maximum performance to help process tasks faster",
               2);

    // Set all cores to maximum performance
    for (unsigned i = 0; i < machineInfo.num_cpus; i++)
    {
        Machine_SetCorePerformance(machine_id, i, P0);
    }
}

void MigrationDone(Time_t time, VMId_t vm_id)
{
    // LogMessage("MigrationDone(): VM " + to_string(vm_id) +
    //   " migration completed at time " + to_string(time), 2);
    scheduler.MarkVMAsReady(vm_id);
    scheduler.MigrationComplete(time, vm_id);
    migrating = false;

    // LogMessage("MigrationDone(): Migration completion processed", 2);
}

void SchedulerCheck(Time_t time)
{
    static unsigned checkCount = 0;
    checkCount++;

    // LogMessage("SchedulerCheck(): Periodic check #" + to_string(checkCount) +
    //           " at time " + to_string(time), 3);

    scheduler.PeriodicCheck(time);

    // Example of triggering a migration - but now with CPU compatibility check
    if (checkCount == 10 && !migrating)
    {
        // Get VM 1's CPU type
        VMId_t vmToMigrate = 1;
        VMInfo_t vmInfo = VM_GetInfo(vmToMigrate);
        CPUType_t vmCpuType = vmInfo.cpu;

        // Find a suitable target machine with the same CPU type
        MachineId_t targetMachine = MachineId_t(-1);

        for (unsigned i = 0; i < Machine_GetTotal(); i++)
        {
            if (i == vmInfo.machine_id)
                continue; // Skip current machine

            MachineInfo_t machineInfo = Machine_GetInfo(MachineId_t(i));
            if (machineInfo.cpu == vmCpuType)
            {
                targetMachine = MachineId_t(i);
                // LogMessage("SchedulerCheck(): Found compatible machine " +
                //           to_string(targetMachine) + " for VM " +
                //           to_string(vmToMigrate), 2);
                break;
            }
        }

        if (targetMachine != MachineId_t(-1))
        {
            // LogMessage("SchedulerCheck(): Triggering migration of VM " +
            //           to_string(vmToMigrate) + " to machine " +
            //           to_string(targetMachine), 1);
            migrating = true;
            scheduler.MarkVMAsMigrating(vmToMigrate);
            VM_Migrate(vmToMigrate, targetMachine);
        }
        else
        {
            // LogMessage("SchedulerCheck(): No compatible machine found for VM " +
            //           to_string(vmToMigrate) + " migration", 1);
        }
    }

    // LogMessage("SchedulerCheck(): Periodic check completed", 3);
}

void SimulationComplete(Time_t time)
{
    LogMessage("SimulationComplete(): Simulation finished at time " + to_string(time), 1);

    scheduler.Shutdown(time);

    LogMessage("SimulationComplete(): Shutdown complete", 1);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
    LogMessage("StateChangeComplete(): Machine " + to_string(machine_id) +
                   " state change completed at time " + to_string(time),
               3);

    // Get the current state of the machine
    MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
    MachineState_t currentState = machineInfo.s_state;

    // Log the new state
    std::string stateStr;
    switch (currentState)
    {
    case S0:
        stateStr = "S0 (Fully On)";
        break;
    case S1:
        stateStr = "S1 (Standby)";
        break;
    case S2:
        stateStr = "S2 (Light Sleep)";
        break;
    case S3:
        stateStr = "S3 (Deep Sleep)";
        break;
    case S4:
        stateStr = "S4 (Hibernation)";
        break;
    case S5:
        stateStr = "S5 (Soft Off)";
        break;
    default:
        stateStr = "Unknown";
        break;
    }

    LogMessage("StateChangeComplete(): Machine " + to_string(machine_id) +
                   " is now in state " + stateStr,
               2);

    // Update scheduler's tracking of active machines
    if (currentState == S0)
    {
        // Machine is now active
        scheduler.ActivateMachine(machine_id);

        // If machine was just powered on, set up initial performance state
        for (unsigned i = 0; i < machineInfo.num_cpus; i++)
        {
            // Start with moderate performance (P1) to balance power and performance
            Machine_SetCorePerformance(machine_id, i, P1);
        }

        // Check if we need to create VMs for this machine
        bool hasVM = false;
        for (VMId_t vm : scheduler.GetVMs())
        {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machine_id)
            {
                hasVM = true;
                break;
            }
        }

        // If no VM exists for this machine, create one
        if (!hasVM)
        {
            LogMessage("StateChangeComplete(): Creating new VM for newly activated machine " +
                           to_string(machine_id),
                       2);

            VMId_t newVM = VM_Create(LINUX, machineInfo.cpu);
            VM_Attach(newVM, machine_id);
            scheduler.AddVM(newVM);

            LogMessage("StateChangeComplete(): Created VM " + to_string(newVM) +
                           " on machine " + to_string(machine_id),
                       2);
        }

        // Check if there are pending tasks that could be assigned to this machine
        LogMessage("StateChangeComplete(): Machine " + to_string(machine_id) +
                       " is ready to accept tasks",
                   2);
    }
    else if (currentState == S5)
    {
        // Machine is now powered off
        scheduler.DeactivateMachine(machine_id);

        LogMessage("StateChangeComplete(): Machine " + to_string(machine_id) +
                       " is now powered off",
                   2);
    }

    // Update energy statistics
    double energyConsumed = Machine_GetEnergy(machine_id);
    LogMessage("StateChangeComplete(): Machine " + to_string(machine_id) +
                   " has consumed " + to_string(energyConsumed) +
                   " energy units so far",
               4);

    // Trigger a periodic check to re-evaluate the system state
    // This is optional but can help adjust to the new machine state
    scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id)
{
    LogMessage("SLAWarning(): Task " + to_string(task_id) +
                   " violated SLA at time " + to_string(time),
               1);

    // Get the SLA type for this task
    SLAType_t slaType = RequiredSLA(task_id);

    // Convert SLA type to string for logging
    std::string slaStr;
    switch (slaType)
    {
    case SLA0:
        slaStr = "SLA0 (95% within 1.2x expected time)";
        break;
    case SLA1:
        slaStr = "SLA1 (90% within 1.5x expected time)";
        break;
    case SLA2:
        slaStr = "SLA2 (80% within 2.0x expected time)";
        break;
    case SLA3:
        slaStr = "SLA3 (best effort)";
        break;
    default:
        slaStr = "Unknown SLA";
        break;
    }

    LogMessage("SLAWarning(): Task " + to_string(task_id) +
                   " has " + slaStr + " requirements",
               2);

    // Find which VM is running this task
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);

    for (VMId_t vm : scheduler.GetVMs())
    {
        VMInfo_t vmInfo = VM_GetInfo(vm);

        for (TaskId_t task : vmInfo.active_tasks)
        {
            if (task == task_id)
            {
                taskVM = vm;
                taskMachine = vmInfo.machine_id;
                break;
            }
        }

        if (taskVM != VMId_t(-1))
            break;
    }

    // Take action based on SLA type
    if (taskVM != VMId_t(-1))
    {
        LogMessage("SLAWarning(): Task " + to_string(task_id) +
                       " is running on VM " + to_string(taskVM) +
                       " on machine " + to_string(taskMachine),
                   2);

        // For strict SLAs (SLA0, SLA1), take immediate action
        if (slaType == SLA0 || slaType == SLA1)
        {
            // 1. Increase task priority to maximum
            SetTaskPriority(task_id, HIGH_PRIORITY);

            // 2. Set machine to maximum performance
            MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
            for (unsigned i = 0; i < machineInfo.num_cpus; i++)
            {
                Machine_SetCorePerformance(taskMachine, i, P0);
            }

            LogMessage("SLAWarning(): Increased priority for task " +
                           to_string(task_id) + " and set machine " +
                           to_string(taskMachine) + " to maximum performance",
                       1);

            // 3. Check if the machine is overloaded
            if (machineInfo.active_tasks > machineInfo.num_cpus * 2)
            {
                // Machine is significantly overloaded, try to migrate this VM

                // Find a less loaded machine with compatible CPU
                MachineId_t targetMachine = MachineId_t(-1);
                VMInfo_t vmInfo = VM_GetInfo(taskVM);
                CPUType_t vmCpuType = vmInfo.cpu;

                for (MachineId_t machine : scheduler.GetMachines())
                {
                    // Skip current machine
                    if (machine == taskMachine)
                        continue;

                    // Skip inactive machines
                    if (!scheduler.IsMachineActive(machine))
                        continue;

                    MachineInfo_t info = Machine_GetInfo(machine);

                    // Check CPU compatibility
                    if (info.cpu != vmCpuType)
                        continue;

                    // Check if machine is less loaded
                    if (info.active_tasks < machineInfo.active_tasks / 2)
                    {
                        targetMachine = machine;
                        LogMessage("SLAWarning(): Found less loaded machine " +
                                       to_string(targetMachine) + " for migration",
                                   2);
                        break;
                    }
                }

                // If no suitable active machine, try to power on a new one
                if (targetMachine == MachineId_t(-1))
                {
                    for (MachineId_t machine : scheduler.GetMachines())
                    {
                        // Skip active machines
                        if (scheduler.IsMachineActive(machine))
                            continue;

                        MachineInfo_t info = Machine_GetInfo(machine);

                        // Check CPU compatibility
                        if (info.cpu != vmCpuType)
                            continue;

                        // Power on the machine
                        LogMessage("SLAWarning(): Powering on machine " +
                                       to_string(machine) + " for VM with SLA-violating task",
                                   1);

                        Machine_SetState(machine, S0);
                        scheduler.ActivateMachine(machine);

                        targetMachine = machine;
                        break;
                    }
                }

                // Perform migration if target found
                if (targetMachine != MachineId_t(-1))
                {
                    LogMessage("SLAWarning(): Migrating VM " + to_string(taskVM) +
                                   " with SLA-violating task to machine " +
                                   to_string(targetMachine),
                               1);

                    scheduler.MarkVMAsMigrating(taskVM);
                    VM_Migrate(taskVM, targetMachine);
                }
                else
                {
                    LogMessage("SLAWarning(): No suitable target machine found for migration", 2);
                }
            }
        }
        // For less strict SLAs, take less aggressive action
        else if (slaType == SLA2)
        {
            // Just increase priority, but not to maximum
            SetTaskPriority(task_id, MID_PRIORITY);

            LogMessage("SLAWarning(): Increased priority for SLA2 task " +
                           to_string(task_id),
                       2);
        }
        // For SLA3, just log the violation
        else
        {
            LogMessage("SLAWarning(): No action taken for best-effort SLA3 task " +
                           to_string(task_id),
                       3);
        }
    }
    else
    {
        LogMessage("SLAWarning(): Could not find VM running task " +
                       to_string(task_id),
                   1);
    }

    // Track SLA violations for reporting
    static std::map<SLAType_t, unsigned> slaViolations;
    slaViolations[slaType]++;

    // Log current violation statistics
    LogMessage("SLAWarning(): Current SLA violation counts - SLA0: " +
                   to_string(slaViolations[SLA0]) + ", SLA1: " +
                   to_string(slaViolations[SLA1]) + ", SLA2: " +
                   to_string(slaViolations[SLA2]),
               3);
}

// Scheduler class implementation
void Scheduler::Init()
{
    LogMessage("Scheduler::Init(): Starting scheduler initialization", 2);

    // Find the parameters of the clusters
    unsigned totalMachines = Machine_GetTotal();
    LogMessage("Scheduler::Init(): Total number of machines is " + to_string(totalMachines), 2);

    // Log machine details for debugging
    LogMessage("Scheduler::Init(): Machine details:", 3);

    // Create a map to track machines by CPU type
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;

    // Initialize all machines and categorize them by CPU type
    for (unsigned i = 0; i < totalMachines; i++)
    {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);

        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;

        // Add to the map by CPU type
        machinesByCPU[cpuType].push_back(machineId);

        if (i < 5)
        { // Limit logging to first 5 for brevity
            LogMessage("  Machine " + to_string(i) + ": CPU=" + to_string(cpuType) +
                           ", Cores=" + to_string(info.num_cpus) +
                           ", Memory=" + to_string(info.memory_size),
                       3);
        }
    }

    // Log the distribution of machines by CPU type
    LogMessage("Scheduler::Init(): Machine distribution by CPU type:", 2);
    for (const auto &pair : machinesByCPU)
    {
        LogMessage("  CPU " + to_string(pair.first) + ": " +
                       to_string(pair.second.size()) + " machines",
                   2);
    }

    // Create VMs for each CPU type
    LogMessage("Scheduler::Init(): Creating VMs for each CPU type", 2);

    // For each CPU type, create at least one VM
    for (const auto &pair : machinesByCPU)
    {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;

        // Skip if no machines with this CPU type
        if (machinesWithCPU.empty())
            continue;

        // Create VMs for this CPU type (at least one, more if many machines)
        unsigned numVMsToCreate = std::min(static_cast<unsigned>(machinesWithCPU.size()), 4u);

        for (unsigned i = 0; i < numVMsToCreate; i++)
        {
            // Create VM with this CPU type
            VMId_t vm = VM_Create(LINUX, cpuType);
            vms.push_back(vm);

            // Attach to a machine with matching CPU
            MachineId_t machine = machinesWithCPU[i % machinesWithCPU.size()];
            VM_Attach(vm, machine);

            // Mark machine as active
            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;

            LogMessage("Scheduler::Init(): Created VM " + to_string(vm) +
                           " with CPU " + to_string(cpuType) +
                           " on machine " + to_string(machine),
                       2);
        }
    }

    // If no VMs were created (unlikely), create some default ones
    if (vms.empty())
    {
        LogMessage("Scheduler::Init(): No VMs created by CPU type, creating defaults", 1);

        for (unsigned i = 0; i < 16 && i < totalMachines; i++)
        {
            MachineId_t machine = MachineId_t(i);
            MachineInfo_t info = Machine_GetInfo(machine);

            VMId_t vm = VM_Create(LINUX, info.cpu);
            vms.push_back(vm);
            VM_Attach(vm, machine);

            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;

            LogMessage("Scheduler::Init(): Created default VM " + to_string(vm) +
                           " on machine " + to_string(machine),
                       2);
        }
    }

    // Turn off machines that don't have VMs attached
    LogMessage("Scheduler::Init(): Setting initial machine states", 2);
    for (MachineId_t machine : machines)
    {
        if (activeMachines.find(machine) == activeMachines.end())
        {
            LogMessage("Scheduler::Init(): Setting machine " + to_string(machine) + " to S5 state", 3);
            Machine_SetState(machine, S5);
        }
    }

    LogMessage("Scheduler::Init(): Initialization complete with " +
                   to_string(vms.size()) + " VMs and " +
                   to_string(activeMachines.size()) + " active machines",
               1);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id)
{
    LogMessage("Scheduler::MigrationComplete(): Processing migration completion for VM " +
                   to_string(vm_id) + " at time " + to_string(time),
               2);

    // Get VM info
    VMInfo_t vmInfo = VM_GetInfo(vm_id);

    // Use size() to get the count of active tasks
    LogMessage("Scheduler::MigrationComplete(): VM " + to_string(vm_id) +
                   " is now on machine " + to_string(vmInfo.machine_id) +
                   " with " + to_string(vmInfo.active_tasks.size()) + " active tasks",
               2);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id)
{
    LogMessage("Scheduler::NewTask(): Processing new task " + to_string(task_id) +
                   " at time " + to_string(now),
               2);

    // Get task requirements
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);

    // Assign priority based on SLA
    Priority_t priority;
    switch (sla_type)
    {
    case SLA0:
        priority = HIGH_PRIORITY; // Highest priority for strictest SLA
        break;
    case SLA1:
        priority = HIGH_PRIORITY;
        break;
    case SLA2:
        priority = MID_PRIORITY;
        break;
    case SLA3:
    default:
        priority = LOW_PRIORITY;
        break;
    }

    LogMessage("Scheduler::NewTask(): Task " + to_string(task_id) +
                   " requires CPU=" + to_string(required_cpu) +
                   ", VM=" + to_string(required_vm),
               3);

    // Find a suitable VM
    VMId_t target_vm = VMId_t(-1);
    unsigned lowest_task_count = UINT_MAX;

    LogMessage("Scheduler::NewTask(): Searching for suitable VM among " +
                   to_string(vms.size()) + " VMs",
               3);

    for (VMId_t vm : vms)
    {
        if (IsVMMigrating(vm))
        {
            LogMessage("Scheduler::NewTask(): Skipping VM " + to_string(vm) +
                           " as it is currently migrating",
                       3);
            continue;
        }
        VMInfo_t info = VM_GetInfo(vm);
        LogMessage("Scheduler::NewTask(): Checking VM " + to_string(vm) +
                       " (CPU=" + to_string(info.cpu) + ")",
                   4);

        if (info.cpu == required_cpu)
        {
            // For SLA0, choose the VM with the fewest active tasks
            if (sla_type == SLA0)
            {
                if (info.active_tasks.size() < lowest_task_count)
                {
                    lowest_task_count = info.active_tasks.size();
                    target_vm = vm;
                }
            }
            // For other SLAs, take the first matching VM
            else
            {
                target_vm = vm;
                break;
            }
        }
    }

    // If no suitable VM found, try to create a new one
    if (target_vm == VMId_t(-1))
    {
        LogMessage("Scheduler::NewTask(): No suitable VM found, looking for machine with CPU " +
                       to_string(required_cpu),
                   2);

        // Find a machine with the required CPU type
        MachineId_t target_machine = MachineId_t(-1);

        // First check active machines
        for (MachineId_t machine : activeMachines)
        {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.cpu == required_cpu)
            {
                target_machine = machine;
                LogMessage("Scheduler::NewTask(): Found active machine " +
                               to_string(machine) + " with required CPU",
                           3);
                break;
            }
        }

        // If no active machine found, check inactive machines
        if (target_machine == MachineId_t(-1))
        {
            for (MachineId_t machine : machines)
            {
                // Skip active machines (already checked)
                if (activeMachines.find(machine) != activeMachines.end())
                    continue;

                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.cpu == required_cpu)
                {
                    target_machine = machine;
                    LogMessage("Scheduler::NewTask(): Found inactive machine " +
                                   to_string(machine) + " with required CPU",
                               3);

                    // Power on the machine
                    LogMessage("Scheduler::NewTask(): Powering on machine " +
                                   to_string(machine),
                               2);
                    Machine_SetState(machine, S0);
                    activeMachines.insert(machine);
                    machineUtilization[machine] = 0.0;
                    break;
                }
            }
        }

        if (target_machine != MachineId_t(-1))
        {
            LogMessage("Scheduler::NewTask(): Creating new VM (Type=" +
                           to_string(required_vm) + ", CPU=" +
                           to_string(required_cpu) + ")",
                       2);

            target_vm = VM_Create(required_vm, required_cpu);
            LogMessage("Scheduler::New Task(): Created VM " + to_string(target_vm), 2);

            LogMessage("Scheduler::NewTask(): Attaching VM " + to_string(target_vm) +
                           " to machine " + to_string(target_machine),
                       2);
            VM_Attach(target_vm, target_machine);

            vms.push_back(target_vm);
        }
        else
        {
            LogMessage("Scheduler::NewTask(): ERROR - No suitable machine found for CPU " +
                           to_string(required_cpu),
                       1);
        }
    }

    // Assign task to VM
    if (target_vm != VMId_t(-1))
    {
        LogMessage("Scheduler::NewTask(): Assigning task " + to_string(task_id) +
                       " to VM " + to_string(target_vm) + " with priority " +
                       to_string(priority),
                   2);

        VM_AddTask(target_vm, task_id, priority);

        LogMessage("Scheduler::NewTask(): Task " + to_string(task_id) +
                       " successfully assigned",
                   2);
    }
    else
    {
        LogMessage("Scheduler::NewTask(): ERROR - Failed to find or create suitable VM for task " +
                       to_string(task_id),
                   1);
    }
}

void Scheduler::PeriodicCheck(Time_t now)
{
    LogMessage("Scheduler::PeriodicCheck(): Starting periodic check at time " +
                   to_string(now),
               3);

    // Log current system state
    unsigned totalActiveTasks = 0;
    unsigned totalActiveVMs = 0;

    for (MachineId_t machine : machines)
    {
        MachineInfo_t info = Machine_GetInfo(machine);
        totalActiveTasks += info.active_tasks;
        totalActiveVMs += info.active_vms;
    }

    LogMessage("Scheduler::PeriodicCheck(): System state - " +
                   to_string(totalActiveVMs) + " active VMs, " +
                   to_string(totalActiveTasks) + " active tasks, " +
                   to_string(activeMachines.size()) + " active machines",
               2);

    // Update machine utilization
    for (MachineId_t machine : machines)
    {
        MachineInfo_t info = Machine_GetInfo(machine);

        // Simple utilization calculation
        double utilization = 0.0;
        if (info.num_cpus > 0)
        {
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        }

        machineUtilization[machine] = utilization;

        if (info.active_tasks > 0)
        {
            LogMessage("Scheduler::PeriodicCheck(): Machine " + to_string(machine) +
                           " utilization: " + to_string(utilization * 100) + "%",
                       4);
        }
    }

    // Example: Adjust CPU performance based on load
    for (MachineId_t machine : machines)
    {
        // Get machine info
        MachineInfo_t info = Machine_GetInfo(machine);

        // Simple logic: if machine has active tasks, set to P0, otherwise P3
        if (info.active_tasks > 0)
        {
            // Set all cores to full performance
            for (unsigned i = 0; i < info.num_cpus; i++)
            {
                Machine_SetCorePerformance(machine, i, P0);
            }
        }
        else
        {
            // Set all cores to low performance
            for (unsigned i = 0; i < info.num_cpus; i++)
            {
                Machine_SetCorePerformance(machine, i, P3);
            }
        }
    }

    // Implement VM consolidation (only if we're not already migrating)
    if (!migrating && now > 1000000)
    { // Wait a bit before starting consolidation
        // Find underutilized machines
        std::vector<MachineId_t> underutilizedMachines;

        for (MachineId_t machine : activeMachines)
        {
            if (machineUtilization[machine] < UNDERLOAD_THRESHOLD)
            {
                underutilizedMachines.push_back(machine);
            }
        }

        // If we have underutilized machines, try to consolidate
        if (!underutilizedMachines.empty())
        {
            LogMessage("Scheduler::PeriodicCheck(): Found " +
                           to_string(underutilizedMachines.size()) +
                           " underutilized machines",
                       2);

            // Try to migrate VMs from the most underutilized machine
            MachineId_t sourceMachine = underutilizedMachines[0];
            MachineInfo_t sourceInfo = Machine_GetInfo(sourceMachine);

            LogMessage("Scheduler::PeriodicCheck(): Attempting to migrate VMs from machine " +
                           to_string(sourceMachine),
                       2);

            // If the machine has VMs, try to migrate one
            if (sourceInfo.active_vms > 0)
            {
                // Find a VM to migrate
                VMId_t vmToMigrate = VMId_t(-1);

                // Get all VMs on this machine
                for (VMId_t vm : vms)
                {
                    VMInfo_t vmInfo = VM_GetInfo(vm);

                    if (vmInfo.machine_id == sourceMachine)
                    {
                        vmToMigrate = vm;
                        break;
                    }
                }

                if (vmToMigrate != VMId_t(-1))
                {
                    // Find a target machine with compatible CPU
                    VMInfo_t vmInfo = VM_GetInfo(vmToMigrate);
                    CPUType_t vmCpuType = vmInfo.cpu;

                    MachineId_t targetMachine = MachineId_t(-1);
                    double bestUtilization = 0.0;

                    // Look for a machine with moderate utilization and compatible CPU
                    for (MachineId_t machine : activeMachines)
                    {
                        if (machine == sourceMachine)
                            continue; // Skip source machine

                        MachineInfo_t machineInfo = Machine_GetInfo(machine);

                        if (machineInfo.cpu == vmCpuType &&
                            machineUtilization[machine] > bestUtilization &&
                            machineUtilization[machine] < OVERLOAD_THRESHOLD)
                        {

                            targetMachine = machine;
                            bestUtilization = machineUtilization[machine];
                        }
                    }

                    if (targetMachine != MachineId_t(-1))
                    {
                        LogMessage("Scheduler::PeriodicCheck(): Migrating VM " +
                                       to_string(vmToMigrate) + " from machine " +
                                       to_string(sourceMachine) + " to machine " +
                                       to_string(targetMachine),
                                   1);

                        migrating = true;
                        MarkVMAsMigrating(vmToMigrate);
                        VM_Migrate(vmToMigrate, targetMachine);
                    }
                    else
                    {
                        LogMessage("Scheduler::PeriodicCheck(): No suitable target machine found for VM " +
                                       to_string(vmToMigrate),
                                   2);
                    }
                }
            }
        }
    }

    LogMessage("Scheduler::PeriodicCheck(): Periodic check completed", 3);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id)
{
    LogMessage("Scheduler::TaskComplete(): Processing task completion for task " +
                   to_string(task_id) + " at time " + to_string(now),
               3);

    // Update machine utilization
    LogMessage("Scheduler::TaskComplete(): Updating machine utilization", 4);

    for (MachineId_t machine : machines)
    {
        MachineInfo_t info = Machine_GetInfo(machine);

        // Simple utilization calculation
        double utilization = 0.0;
        if (info.num_cpus > 0)
        {
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        }

        machineUtilization[machine] = utilization;
    }

    LogMessage("Scheduler::TaskComplete(): Task completion processing finished", 3);
}

void Scheduler::Shutdown(Time_t now)
{
    LogMessage("Scheduler::Shutdown(): Starting shutdown at time " + to_string(now), 1);

    // Report energy statistics
    double totalEnergy = Machine_GetClusterEnergy();
    LogMessage("Scheduler::Shutdown(): Total energy consumed: " +
                   to_string(totalEnergy) + " KW-Hour",
               1);

    // Report active machines at end
    LogMessage("Scheduler::Shutdown(): Active machines at end: " +
                   to_string(activeMachines.size()) + " out of " +
                   to_string(machines.size()),
               1);

    // Report SLA statistics - use stringstream to properly concatenate strings with doubles
    LogMessage("Scheduler::Shutdown(): SLA violation statistics:", 1);

    std::stringstream sla0, sla1, sla2, sla3;
    sla0 << "  SLA0: " << GetSLAReport(SLA0) << "%";
    sla1 << "  SLA1: " << GetSLAReport(SLA1) << "%";
    sla2 << "  SLA2: " << GetSLAReport(SLA2) << "%";
    sla3 << "  SLA3: " << GetSLAReport(SLA3) << "%";

    LogMessage(sla0.str(), 1);
    LogMessage(sla1.str(), 1);
    LogMessage(sla2.str(), 1);
    LogMessage(sla3.str(), 1);

    // Report detailed machine statistics
    LogMessage("Scheduler::Shutdown(): Machine statistics:", 1);
    for (MachineId_t machine : machines)
    {
        MachineInfo_t info = Machine_GetInfo(machine);
        double energy = Machine_GetEnergy(machine);

        LogMessage("  Machine " + to_string(machine) +
                       ": Energy=" + to_string(energy) +
                       ", Active Tasks=" + to_string(info.active_tasks) +
                       ", Active VMs=" + to_string(info.active_vms),
                   2);
    }

    LogMessage("Scheduler::Shutdown(): Shutdown complete", 1);
}