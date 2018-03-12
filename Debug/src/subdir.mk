################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/run.cpp 

OBJS += \
./src/run.o 

CPP_DEPS += \
./src/run.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I/usr/local/hadoop-2.7.4/src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs -I/usr/java/jdk1.8.0_151/include -I/usr/java/jdk1.8.0_151/include/linux -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


