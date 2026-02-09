import asyncio
import json
import logging
from datetime import datetime
from confluent_kafka import KafkaError

from config import logger, KAFKA_BOOTSTRAP
from modules.kafka_handler import KafkaHandler
from modules.llm_engine import LLMEngine

async def main():
    # 1. 모듈 초기화
    kafka = KafkaHandler()
    llm_engine = LLMEngine()

    # 2. MCP 서버(Tool) 연결
    if not await llm_engine.connect_mcp():
        logger.error("프로그램을 종료합니다.")
        return

    logger.info(f"🎧 Kafka Consumer 시작 ({KAFKA_BOOTSTRAP})")

    try:
        while True:
            # Kafka Poll (Non-blocking 방식이 좋으나, 여기서는 loop 내 blocking poll 사용)
            # asyncio 환경에서는 run_in_executor 등을 고려할 수 있으나, 
            # 단순화를 위해 짧은 타임아웃으로 반복합니다.
            msg = kafka.consumer.poll(0.5)
            
            if msg is None:
                await asyncio.sleep(0.1) # CPU 점유 방지
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka Error: {msg.error()}")
                continue

            # 메시지 처리
            try:
                start_time = datetime.now()
                data = json.loads(msg.value().decode('utf-8'))
                
                user_text = data.get("text") or data.get("question")
                logger.info(f"📩 요청 수신: {user_text}")

                if user_text:
                    # LLM 처리
                    answer = await llm_engine.process_text(user_text)
                    
                    # 소요 시간 계산
                    diff = (datetime.now() - start_time).total_seconds()
                    
                    # 응답 전송
                    kafka.send_response(data, answer, diff)

            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {e}")

    except KeyboardInterrupt:
        logger.info("종료 요청 받음.")
    finally:
        kafka.close()
        await llm_engine.cleanup()

if __name__ == "__main__":
    asyncio.run(main())