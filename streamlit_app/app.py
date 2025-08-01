import streamlit as st
import json
import pandas as pd
from PIL import Image, ImageOps, ImageDraw
import invoke_agent_utils as agenthelper

# Page configuration
st.set_page_config(page_title="Text2SQL Agent", page_icon=":robot_face:", layout="wide")

class StreamlitApp:
    def __init__(self):
        self.session_id = "MYSESSION"
        self.init_session_state()

    def init_session_state(self):
        """Initialize session state variables"""
        if 'history' not in st.session_state:
            st.session_state['history'] = []

    def crop_to_circle(self, image):
        """Crop image into a circle"""
        mask = Image.new('L', image.size, 0)
        mask_draw = ImageDraw.Draw(mask)
        mask_draw.ellipse((0, 0) + image.size, fill=255)
        result = ImageOps.fit(image, mask.size, centering=(0.5, 0.5))
        result.putalpha(mask)
        return result

    def format_response(self, response_body):
        """Parse and format response"""
        try:
            data = json.loads(response_body)
            return pd.DataFrame(data) if isinstance(data, list) else response_body
        except json.JSONDecodeError:
            return response_body

    def handle_submit(self, prompt):
        """Handle form submission"""
        event = {"sessionId": self.session_id, "question": prompt}
        response = agenthelper.lambda_handler(event, None)
        
        try:
            if response and 'body' in response and response['body']:
                response_data = json.loads(response['body'])
                all_data = self.format_response(response_data['response'])
                the_response = response_data['trace_data']
            else:
                all_data = "..."
                the_response = "Error occurred. Please try again."
        except (json.JSONDecodeError, KeyError):
            all_data = "..."
            the_response = "Error occurred. Please try again."

        st.sidebar.text_area("", value=all_data, height=300)
        st.session_state['history'].append({"question": prompt, "answer": the_response})

    def handle_end_session(self):
        """Handle session end"""
        st.session_state['history'].append({
            "question": "Session Ended", 
            "answer": "Thank you for using Text2SQL Agent!"
        })
        event = {"sessionId": self.session_id, "question": "end", "endSession": True}
        agenthelper.lambda_handler(event, None)
        st.session_state['history'].clear()


    def display_conversation_history(self):
        """Display conversation history"""
        st.write("## Histórico da Conversa")

        # Usamos enumerate para obter um índice único para cada item no histórico
        # E reversed para mostrar as conversas mais recentes primeiro
        for i, chat in enumerate(reversed(st.session_state['history'])):
            # Pergunta
            col1_q, col2_q = st.columns([2, 10])
            with col1_q:
                try:
                    # Carregamento de imagens pode falhar se URL for inacessível ou formato inválido
                    human_image = Image.open('images/luke.jpg')
                    circular_human_image = self.crop_to_circle(human_image)
                    st.image(circular_human_image, width=125)
                except Exception as e: # Captura a exceção para evitar quebrar o app
                    st.write("👤")
                    st.warning(f"Erro ao carregar imagem do usuário: {e}") # Adicionado para debug

            with col2_q:
                # Geramos uma chave única usando o índice 'i'
                st.text_area(
                    label="Pergunta do Usuário:", # Label descritivo
                    value=chat["question"],
                    height=68,
                    key=f"question_chat_{i}", # Chave única agora
                    disabled=True,
                    label_visibility="hidden" # Oculta o label se você não quiser que apareça na UI
                )

            # Resposta
            col1_a, col2_a = st.columns([2, 10])
            with col1_a:
                try:
                    # Carregamento de imagens pode falhar
                    robot_image = Image.open('images/r2d2.jpg')
                    circular_robot_image = self.crop_to_circle(robot_image)
                    st.image(circular_robot_image, width=150)
                except Exception as e: # Captura a exceção para evitar quebrar o app
                    st.write("🤖")
                    st.warning(f"Erro ao carregar imagem do robô: {e}") # Adicionado para debug

            with col2_a:
                if isinstance(chat["answer"], pd.DataFrame):
                    st.dataframe(chat["answer"], use_container_width=True) # Melhorar visualização de dataframe
                else:
                    # Geramos uma chave única para a resposta
                    st.text_area(
                        label="Resposta do Sistema:", # Label descritivo
                        value=chat["answer"],
                        height=100,
                        key=f"answer_chat_{i}", # Chave única agora
                        label_visibility="hidden" # Oculta o label
                    )
            st.markdown("---") # Adiciona uma linha divisória para separar os chats

    def run(self):
        """Main application logic"""
        st.title("Hospital DataHandsON - TextToSQL")
        st.sidebar.title("Trace Data")

        # Input form
        prompt = st.text_input("Please enter your query?", max_chars=2000).strip()
        submit_button = st.button("Submit", type="primary")
        #end_session_button = st.button("End Session")

        # Handle interactions
        if submit_button and prompt:
            self.handle_submit(prompt)

        # if end_session_button:
        #     self.handle_end_session()

        # Display components
        self.display_conversation_history()

# Run the app
if __name__ == "__main__":
    app = StreamlitApp()
    app.run()